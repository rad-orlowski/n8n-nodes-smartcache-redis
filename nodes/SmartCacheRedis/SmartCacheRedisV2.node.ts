/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2026, Rad Orlowski
 *
 * V2 — single-input design.
 *
 * V1's two declared "main" input slots (Input, Write) make n8n's core execution
 * engine treat the node as a join/barrier: it waits for data (or an explicit
 * "unreachable" signal) on EVERY declared input before running the node at all.
 * The documented usage loops the expensive node's result back into Write — a
 * genuine cycle — which the engine can never resolve for `executionOrder: 'v1'`
 * workflows (the modern default), so the node silently never executes and
 * anything wired only through it is dropped, with the run still reporting
 * "success".
 *
 * V2 fixes this with a single input slot. Both the original data source AND the
 * downstream "expensive node" result wire into the SAME input anchor (n8n
 * supports multiple incoming wires converging on one input index — this is the
 * same pattern n8n's own "Loop Over Items" node uses to loop into itself).
 * "Is this item's write-back still outstanding" is tracked in-band via a
 * `pending` flag on the node-scoped per-item cache-metadata object (keyed by
 * pairedItem index) instead of via a second physical input.
 */

import type {
  IContextObject,
  IExecuteFunctions,
  INodeExecutionData,
  INodeType,
  INodeTypeDescription,
} from 'n8n-workflow'
import { NodeConnectionTypes, NodeOperationError } from 'n8n-workflow'

import { RedisBackend } from './redisStorage'
import type { CacheBackend } from './storage'
import { getItemIndex, processBatch, processSingleItem, writeToCache } from './shared'

export interface SmartCacheV2Params {
  input: INodeExecutionData[]
  context: IContextObject
  batchMode: boolean
  force: boolean
  cacheKeyFields: string
  cacheDir: string
  ttl: number
  logger: IExecuteFunctions['logger']
  nodeId: string
  backend: CacheBackend
  /** Node reference for NodeOperationError construction — omit in pure unit tests. */
  getNode: () => ConstructorParameters<typeof NodeOperationError>[0]
}

/**
 * Pure core of V2's algorithm — everything execute() does except constructing
 * the Redis connection. Kept as a standalone function (rather than inlined in
 * execute()) so it can be unit-tested against an in-memory CacheBackend across
 * multiple simulated loop iterations, the same way shared.ts's helpers are.
 */
export const processSmartCacheV2 = async (
  params: SmartCacheV2Params,
): Promise<INodeExecutionData[][]> => {
  const {
    input,
    context,
    batchMode,
    force,
    cacheKeyFields,
    cacheDir,
    ttl,
    logger,
    nodeId,
    backend,
    getNode,
  } = params

  if (input.length === 0) {
    logger.debug('[SmartCacheRedis] Input empty, returning early')
    return [[], []]
  }

  // Split the single input into items already written to cache (looped back
  // through the expensive node) and genuinely new items — driven purely by
  // the in-band `pending` flag on this node's stored per-item cache metadata,
  // NOT by which physical wire the item arrived on (there is only one).
  const writeBackItems: INodeExecutionData[] = []
  const freshItems: INodeExecutionData[] = []
  for (const item of input) {
    const idx = getItemIndex(item.pairedItem)
    if (context[idx]?.pending === true) {
      writeBackItems.push(item)
    } else {
      freshItems.push(item)
    }
  }

  const passThroughResults: INodeExecutionData[] = []

  if (writeBackItems.length > 0) {
    if (batchMode) {
      try {
        await writeToCache(writeBackItems, context, backend)
      } catch (err) {
        throw new NodeOperationError(
          getNode(),
          `Failed to persist cache to Redis: ${String(err instanceof Error ? err.message : err)}`,
        )
      }
      writeBackItems.forEach((item) => {
        const idx = getItemIndex(item.pairedItem)
        if (context[idx]) context[idx].pending = false
      })
      passThroughResults.push(...writeBackItems)
    } else {
      for (const item of writeBackItems) {
        try {
          await writeToCache(item, context, backend)
        } catch (err) {
          throw new NodeOperationError(
            getNode(),
            `Failed to persist cache to Redis: ${String(err instanceof Error ? err.message : err)}`,
          )
        }
        const idx = getItemIndex(item.pairedItem)
        if (context[idx]) context[idx].pending = false
        passThroughResults.push(item)
      }
    }

    logger.debug('[SmartCacheRedis] Finished processing write-back items:', {
      count: writeBackItems.length,
    })
  }

  let hits: INodeExecutionData[] = []
  let misses: INodeExecutionData[] = []

  if (freshItems.length > 0) {
    const result = batchMode
      ? await processBatch(
          freshItems,
          context,
          cacheKeyFields,
          cacheDir,
          force,
          ttl,
          logger,
          nodeId,
          backend,
        )
      : await Promise.all(
          freshItems.map((item) =>
            processSingleItem(
              item,
              context,
              cacheKeyFields,
              cacheDir,
              force,
              ttl,
              logger,
              nodeId,
              backend,
            ),
          ),
        ).then((results) => ({
          hits: results.filter((r) => r.hit).map((r) => r.hit!),
          misses: results.filter((r) => r.miss).map((r) => r.miss!),
        }))

    hits = result.hits
    misses = result.misses

    // Mark every miss as awaiting write-back BEFORE it's emitted, so when it
    // loops back through the expensive node into this same input, it's
    // correctly reclassified as a write-back on the next call.
    misses.forEach((item) => {
      const idx = getItemIndex(item.pairedItem)
      if (context[idx]) context[idx].pending = true
    })

    logger.debug('[SmartCacheRedis] Finished processing fresh items:', {
      totalItems: freshItems.length,
      cacheHits: hits.length,
      cacheMisses: misses.length,
    })
  }

  return [[...hits, ...passThroughResults], [...misses]]
}

export class SmartCacheRedisV2 implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Smart Cache (Redis)',
    name: 'smartCacheRedis',
    icon: 'file:smartCacheRedis.svg',
    group: ['transform'],
    version: 2,
    description:
      'Intelligent caching node with automatic hash generation and TTL support. Persists cache entries to Redis with native key expiry.',
    subtitle:
      '={{ ($parameter["batchMode"] ? "Batch" : "Individual") + ($parameter["force"] ? " • ⚠️ Force Miss" : "") }}',
    documentationUrl: 'https://github.com/rad-orlowski/n8n-nodes-smartcache-redis#readme',
    defaults: {
      name: 'Smart Cache (Redis)',
    },
    inputs: [
      {
        displayName: 'Input',
        type: NodeConnectionTypes.Main,
        required: true,
      },
    ],
    outputs: [
      {
        displayName: 'Cache Hit',
        type: NodeConnectionTypes.Main,
      },
      {
        displayName: 'Cache Miss',
        type: NodeConnectionTypes.Main,
        required: true,
      },
    ],
    credentials: [
      {
        name: 'redis',
        required: true,
      },
    ],
    properties: [
      {
        displayName: 'Key Prefix',
        name: 'cacheDir',
        type: 'string',
        default: 'smartcache',
        description:
          'Prefix for Redis keys (use different prefixes to separate caches, e.g. per workflow)',
        noDataExpression: true,
      },
      {
        displayName: 'Batch Mode',
        name: 'batchMode',
        type: 'boolean',
        default: false,
        description:
          'Whether to process all input items as a single unit for caching, similar to "Run Once for All Items"',
      },
      {
        displayName: 'Force Miss',
        name: 'force',
        type: 'boolean',
        default: false,
        description:
          'Whether to force cache miss and regeneration of data, ignoring any existing cache',
      },
      {
        displayName: 'Cache Key Fields',
        name: 'cacheKeyFields',
        type: 'string',
        default: '',
        placeholder: 'id,name,url,user.id',
        description:
          'Comma-separated list of fields to use for cache key generation. Supports dot notation for nested fields (e.g. user.id). Leave empty to use entire input for more precise caching.',
      },
      {
        displayName: 'TTL (Seconds)',
        name: 'ttl',
        type: 'number',
        default: 86400,
        description:
          'Time-to-live for cache entries in seconds, applied as a native Redis key expiry. Use 0 for infinite.',
      },
    ],
  }

  async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
    const force = this.getNodeParameter('force', 0) as boolean
    const ttl = this.getNodeParameter('ttl', 0) as number
    const cacheDir = this.getNodeParameter('cacheDir', 0) as string
    const cacheKeyFields = (this.getNodeParameter('cacheKeyFields', 0) as string).trim()
    const batchMode = this.getNodeParameter('batchMode', 0) as boolean
    const context = this.getContext('node')
    if (force) {
      this.logger.warn(
        '[SmartCacheRedis] Force Miss is enabled: cache reads will be bypassed and new objects will be written',
      )
    }

    const creds = (await this.getCredentials('redis')) as unknown as {
      host: string
      port: number
      database: number
      password?: string
      user?: string
      ssl?: boolean
      disableTlsVerification?: boolean
    }
    if (!creds) {
      throw new NodeOperationError(this.getNode(), 'Redis credentials are required')
    }

    const backend: RedisBackend = new RedisBackend(
      {
        host: String(creds.host || 'localhost'),
        port: Number(creds.port || 6379),
        database: Number(creds.database ?? 0),
        password: creds.password ? String(creds.password) : undefined,
        user: creds.user ? String(creds.user) : undefined,
        ssl: Boolean(creds.ssl),
        disableTlsVerification: Boolean(creds.disableTlsVerification),
      },
      ttl,
    )

    try {
      await backend.ensureConnection()
    } catch (err) {
      await backend.quit()
      throw new NodeOperationError(
        this.getNode(),
        `Could not connect to Redis: ${err instanceof Error ? err.message : String(err)}`,
      )
    }

    const input = this.getInputData(0)

    this.logger.debug('[SmartCacheRedis] Initialized with parameters:', {
      force,
      ttl,
      cachePrefix: cacheDir,
      cacheKeyFields,
    })

    try {
      return await processSmartCacheV2({
        input,
        context,
        batchMode,
        force,
        cacheKeyFields,
        cacheDir,
        ttl,
        logger: this.logger,
        nodeId: this.getNode().id,
        backend,
        getNode: () => this.getNode(),
      })
    } finally {
      // Close the Redis connection so executions don't leak sockets
      await backend.quit()
    }
  }
}
