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
 *
 * Pure helpers shared between SmartCacheRedisV1 and SmartCacheRedisV2 — moved
 * out of the node file unchanged (V1 imports these; its behavior does not
 * change by even one bit) so V2 can reuse the same cache read/write logic.
 */

import { createHash } from 'node:crypto'
import type {
  IContextObject,
  IExecuteFunctions,
  INodeExecutionData,
} from 'n8n-workflow'
import { ApplicationError } from 'n8n-workflow'

import { CacheBackend, joinPrefix } from './storage'

export const getItemIndex = (pairedItem: INodeExecutionData['pairedItem']): number => {
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

// Resolves a dotted path ("user.id") against a JSON value. Non-object/nullish
// intermediates resolve to undefined rather than throwing.
const getFieldByPath = (source: unknown, path: string): unknown =>
  path
    .split('.')
    .map((segment) => segment.trim())
    .reduce<unknown>(
      (value, segment) =>
        value != null && typeof value === 'object'
          ? (value as Record<string, unknown>)[segment]
          : undefined,
      source,
    )

export const processItemData = (item: INodeExecutionData, cacheKeyFields: string) =>
  cacheKeyFields
    ? cacheKeyFields.split(',').reduce(
        (acc, field) => {
          const path = field.trim()
          acc[path] = getFieldByPath(item.json, path)
          return acc
        },
        {} as Record<string, unknown>,
      )
    : item.json

export const generateCacheMetadata = (
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

export const writeToCache = async (
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

export const handleCacheHit = async (
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

export const processBatch = async (
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
      const cached = Array.isArray(result.content) ? result.content : [result.content]
      // Cache content was written by a PRIOR execution and carries that execution's
      // pairedItem indices. Those indices are meaningless (or point at the wrong item)
      // in the CURRENT execution's input array — re-stamp each hit to the current item
      // it corresponds to, so nodes wired to "Cache Hit" can trace lineage back through
      // this node's own "Input" connection.
      const hits = cached.map((hitItem, i) => {
        const currentItem = items[i]
        return currentItem
          ? { ...hitItem, pairedItem: { item: getItemIndex(currentItem.pairedItem) } }
          : hitItem
      })
      return { hits, misses: [] }
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

export const processSingleItem = async (
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
      // Same reason as the batch path above: re-stamp pairedItem to this execution's
      // item index rather than replaying the write-time execution's stale index.
      const cached = result.content as INodeExecutionData
      return { hit: { ...cached, pairedItem: { item: itemIndex } }, miss: null }
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
