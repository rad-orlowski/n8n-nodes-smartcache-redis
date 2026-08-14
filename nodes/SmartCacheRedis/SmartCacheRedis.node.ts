/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2025, Victor Duarte
 * Copyright (c) 2026, Rad Orlowski — Redis backend adaptation
 *
 * Adapted from n8n-nodes-smartcache (https://github.com/skadaai/n8n-nodes-smartcache)
 * S3 storage replaced with Redis (native TTL, built-in `redis` credential).
 */

import { createHash } from 'node:crypto'
import {
  IContextObject,
  IExecuteFunctions,
  INodeExecutionData,
  INodeType,
  INodeTypeDescription,
  NodeConnectionTypes,
  NodeOperationError,
  ApplicationError,
} from 'n8n-workflow'

import { CacheBackend, joinPrefix } from './storage'
import { RedisBackend } from './redisStorage'

const getItemIndex = (pairedItem: INodeExecutionData['pairedItem']): number => {
  if (Array.isArray(pairedItem)) {
    return pairedItem[0]?.item ?? 0
  }

  if (typeof pairedItem === 'object') {
    return pairedItem?.item ?? 0
  }

  if (pairedItem === undefined || pairedItem === null) {
    throw new ApplicationError('PairedItem index cannot be undefined')
  }
  return pairedItem as number
}

const getCachePathFromItem = (item: INodeExecutionData, context: IContextObject) => {
  const ctx = context[getItemIndex(item.pairedItem)]
  if (!ctx) {
    throw new ApplicationError('Context not found in input data')
  }
  const cachePath = ctx.cachePath
  if (!cachePath) {
    throw new ApplicationError('Cache path not found in input data')
  }
  return cachePath
}

const processItemData = (item: INodeExecutionData, cacheKeyFields: string) =>
  cacheKeyFields
    ? cacheKeyFields.split(',').reduce(
        (acc, field) => {
          acc[field.trim()] = item.json[field.trim()]
          return acc
        },
        {} as Record<string, unknown>,
      )
    : item.json

const generateCacheMetadata = (
  items: INodeExecutionData | INodeExecutionData[],
  cacheKeyFields: string,
  prefix: string,
  nodeId: string,
) => {
  const dataToHash = Array.isArray(items)
    ? items.map((item) => processItemData(item, cacheKeyFields))
    : processItemData(items, cacheKeyFields)

  // Sort keys to ensure consistent hash generation
  const sortedData = Array.isArray(dataToHash)
    ? dataToHash.map((item) =>
        Object.keys(item)
          .sort()
          .reduce(
            (acc, key) => {
              acc[key] = item[key]
              return acc
            },
            {} as Record<string, unknown>,
          ),
      )
    : Object.keys(dataToHash)
        .sort()
        .reduce(
          (acc, key) => {
            acc[key] = dataToHash[key]
            return acc
          },
          {} as Record<string, unknown>,
        )

  // Include nodeId in hash generation to separate caches for different node instances
  const hash = createHash('sha256')
    .update(JSON.stringify({ nodeId, data: sortedData }))
    .digest('hex')
  const objectKey = joinPrefix(prefix, `${hash}.cache`)

  return {
    cacheKey: hash,
    cachePath: objectKey,
  }
}

const writeToCache = async (
  items: INodeExecutionData | INodeExecutionData[],
  context: IContextObject,
  backend?: CacheBackend,
) => {
  // If array, any item serves as they all share the same $smartcache object
  const firstItem = Array.isArray(items) ? items[0] : items
  if (!firstItem) {
    throw new ApplicationError('Items cannot be empty')
  }
  const cachePath = getCachePathFromItem(firstItem, context)
  if (!backend) {
    throw new ApplicationError('Cache backend not available')
  }
  await backend.put(cachePath, items)
  console.debug(`[SmartCacheRedis] Wrote to cache at ${cachePath}`)
}

const handleCacheHit = async (
  cachePath: string,
  ttl: number,
  backend: CacheBackend,
) => {
  const head = await backend.head(cachePath)
  if (!head) return { status: 'miss' as const }
  if (ttl > 0) {
    const cacheAge = (Date.now() - head.lastModified.getTime()) / 1000
    if (cacheAge >= ttl) {
      return { status: 'expired' as const, cacheAge }
    }
  }
  const content = await backend.get(cachePath)
  if (content == null) return { status: 'miss' as const }
  return { status: 'hit' as const, content }
}

const processBatch = async (
  items: INodeExecutionData[],
  context: IContextObject,
  cacheKeyFields: string,
  prefix: string,
  force: boolean,
  ttl: number,
  logger: IExecuteFunctions['logger'],
  nodeId: string,
  backend: CacheBackend,
) => {
  const $smartCache = generateCacheMetadata(items, cacheKeyFields, prefix, nodeId)

  // Store cache metadata for each item
  items.forEach((item) => {
    const itemIndex = getItemIndex(item.pairedItem)
    context[itemIndex] = $smartCache
  })

  logger.debug('[SmartCacheRedis] Generated batch cache metadata:', {
    cacheKey: $smartCache.cacheKey,
    cachePath: $smartCache.cachePath,
    hashedData: cacheKeyFields ? 'Selected JSON fields' : 'Full JSON',
    itemCount: items.length,
  })

  if (force) {
    return { hits: [], misses: items }
  }

  try {
    const result = await handleCacheHit($smartCache.cachePath, ttl, backend)
    if (result.status === 'hit') {
      return { hits: Array.isArray(result.content) ? result.content : [result.content], misses: [] }
    }
    return { hits: [], misses: items }
  } catch (error) {
    logger.debug('[SmartCacheRedis] Batch cache miss:', {
      cacheKey: $smartCache.cacheKey,
      error: error instanceof Error ? error.message : String(error),
    })
    return { hits: [], misses: items }
  }
}

const processSingleItem = async (
  item: INodeExecutionData,
  context: IContextObject,
  cacheKeyFields: string,
  prefix: string,
  force: boolean,
  ttl: number,
  logger: IExecuteFunctions['logger'],
  nodeId: string,
  backend: CacheBackend,
) => {
  const $smartCache = generateCacheMetadata(item, cacheKeyFields, prefix, nodeId)
  const itemIndex = getItemIndex(item.pairedItem)
  context[itemIndex] = $smartCache

  logger.debug('[SmartCacheRedis] Generated cache metadata:', {
    cacheKey: $smartCache.cacheKey,
    cachePath: $smartCache.cachePath,
    hashedData: cacheKeyFields ? 'Selected JSON fields' : 'Full JSON',
    itemJson: item.json,
  })

  if (force) {
    return { hit: null, miss: item }
  }

  try {
    const result = await handleCacheHit($smartCache.cachePath, ttl, backend)
    if (result.status === 'hit') {
      return { hit: result.content, miss: null }
    }
    return { hit: null, miss: item }
  } catch (error) {
    logger.debug('[SmartCacheRedis] Cache miss:', {
      cacheKey: $smartCache.cacheKey,
      error: error instanceof Error ? error.message : String(error),
    })
    return { hit: null, miss: item }
  }
}

export class SmartCacheRedis implements INodeType {
  description: INodeTypeDescription = {
    displayName: 'Smart Cache (Redis)',
    name: 'smartCacheRedis',
    icon: 'file:smartCacheRedis.svg',
    group: ['transform'],
    version: 1,
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
      {
        displayName: 'Write',
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
        placeholder: 'id,name,url',
        description:
          'Comma-separated list of fields to use for cache key generation. Leave empty to use entire input for more precise caching.',
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

    const mainInput = this.getInputData(0) // Input 1
    const cacheInput = this.getInputData(1) // Input 2 (write)

    this.logger.debug('[SmartCacheRedis] Initialized with parameters:', {
      force,
      ttl,
      cachePrefix: cacheDir,
      cacheKeyFields,
    })

    try {
      // Early return if both inputs are empty
      if (mainInput.length === 0 && cacheInput.length === 0) {
        this.logger.debug('[SmartCacheRedis] Both inputs empty, returning early')
        return [[], []]
      }

      // Process main input
      if (mainInput.length > 0) {
        const nodeId = this.getNode().id
        const { hits, misses } = batchMode
          ? await processBatch(
              mainInput,
              context,
              cacheKeyFields,
              cacheDir,
              force,
              ttl,
              this.logger,
              nodeId,
              backend,
            )
          : await Promise.all(
              mainInput.map((item) =>
                processSingleItem(
                  item,
                  context,
                  cacheKeyFields,
                  cacheDir,
                  force,
                  ttl,
                  this.logger,
                  nodeId,
                  backend,
                ),
              ),
            ).then((results) => ({
              hits: results.filter((r) => r.hit).map((r) => r.hit!),
              misses: results.filter((r) => r.miss).map((r) => r.miss!),
            }))

        this.logger.debug('[SmartCacheRedis] Finished processing main input:', {
          totalItems: mainInput.length,
          cacheHits: hits.length,
          cacheMisses: misses.length,
        })

        return [hits, misses]
      }

      // Process cache writes
      if (cacheInput.length > 0) {
        if (batchMode) {
          const firstItem = cacheInput[0]
          if (!firstItem) {
            throw new NodeOperationError(this.getNode(), 'Cache input cannot be empty')
          }
          if (getItemIndex(firstItem.pairedItem) === undefined) {
            throw new NodeOperationError(
              this.getNode(),
              'Write input items must come from cache miss output',
            )
          }
          try {
            await writeToCache(cacheInput, context, backend)
          } catch (err) {
            throw new NodeOperationError(
              this.getNode(),
              `Failed to persist cache to Redis: ${String(err instanceof Error ? err.message : err)}`,
            )
          }
          return [cacheInput, []]
        }

        const results: INodeExecutionData[] = []
        for (const item of cacheInput) {
          if (getItemIndex(item.pairedItem) === undefined) {
            throw new NodeOperationError(
              this.getNode(),
              'Write input items must come from cache miss output',
            )
          }
          try {
            await writeToCache(item, context, backend)
          } catch (err) {
            throw new NodeOperationError(
              this.getNode(),
              `Failed to persist cache to Redis: ${String(err instanceof Error ? err.message : err)}`,
            )
          }
          results.push(item)
        }

        this.logger.debug('[SmartCacheRedis] Finished processing cache writes')
        return [results, []]
      }

      return [[], []]
    } finally {
      // Close the Redis connection so executions don't leak sockets
      await backend.quit()
    }
  }
}
