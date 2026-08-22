/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2026, Rad Orlowski
 */

import { describe, expect, test } from 'vitest'
import type { IContextObject, INode, INodeExecutionData } from 'n8n-workflow'

import type { CacheBackend } from '../storage'
import { processSmartCacheV2 } from '../SmartCacheRedisV2.node'

/** Real (non-mock) in-memory implementation of the CacheBackend interface, matching
 * what RedisBackend does: round-trip whatever is put(), pairedItem included. */
class InMemoryBackend implements CacheBackend {
  private store = new Map<string, { data: unknown; ts: number }>()

  async head(key: string) {
    const entry = this.store.get(key)
    return entry ? { lastModified: new Date(entry.ts) } : null
  }

  async get<T>(key: string): Promise<T | null> {
    const entry = this.store.get(key)
    return entry ? (entry.data as T) : null
  }

  async put<T>(key: string, value: T): Promise<void> {
    this.store.set(key, { data: value, ts: Date.now() })
  }
}

const noopLogger = {
  debug: () => {},
  info: () => {},
  warn: () => {},
  error: () => {},
} as unknown as import('n8n-workflow').Logger

const fakeNode: INode = {
  id: 'node-1',
  name: 'Smart Cache (Redis)',
  typeVersion: 2,
  type: 'smartCacheRedis',
  position: [0, 0],
  parameters: {},
}

/** Base params shared by every call in a test, overridable per-call. */
const baseParams = (context: IContextObject, backend: CacheBackend, overrides: Partial<{
  batchMode: boolean
  force: boolean
  cacheKeyFields: string
  cacheDir: string
  ttl: number
}> = {}) => ({
  context,
  batchMode: overrides.batchMode ?? false,
  force: overrides.force ?? false,
  cacheKeyFields: overrides.cacheKeyFields ?? '',
  cacheDir: overrides.cacheDir ?? 'smartcache',
  ttl: overrides.ttl ?? 0,
  logger: noopLogger,
  nodeId: 'node-1',
  backend,
  getNode: () => fakeNode,
})

describe('SmartCacheRedisV2 — write-back state does not leak across loop iterations', () => {
  test('an index reused by an unrelated later loop iteration starts fresh, not misclassified as a leftover write-back', async () => {
    const backend = new InMemoryBackend()
    const context: IContextObject = {}

    // --- Call 1: brand-new item at index 0, cache is empty -> miss ---
    const itemA: INodeExecutionData = { json: { payload: 'A' }, pairedItem: { item: 0 } }
    const call1 = await processSmartCacheV2({
      ...baseParams(context, backend),
      input: [itemA],
    })
    expect(call1[0]).toEqual([]) // Cache Hit
    expect(call1[1]).toHaveLength(1) // Cache Miss
    expect(context[0]?.pending).toBe(true)

    // --- Call 2: the expensive node's result for item A loops back into the
    // SAME single input, still carrying pairedItem index 0 ---
    const itemAResult: INodeExecutionData = {
      json: { payload: 'A', processed: true },
      pairedItem: { item: 0 },
    }
    const call2 = await processSmartCacheV2({
      ...baseParams(context, backend),
      input: [itemAResult],
    })
    expect(call2[1]).toEqual([]) // nothing on Cache Miss
    expect(call2[0]).toHaveLength(1) // passed through on Cache Hit
    expect(call2[0][0]).toEqual(itemAResult)
    // Self-cleaning: pending is cleared the moment the write-back is consumed
    expect(context[0]?.pending).toBe(false)

    // --- Call 3: an UNRELATED later loop iteration reuses pairedItem index 0
    // for entirely different data (e.g. the next item in a Split In Batches
    // loop). This must be treated as a brand-new item, not a leftover
    // write-back — proving no state leaked from the previous iteration. ---
    const itemB: INodeExecutionData = { json: { payload: 'B' }, pairedItem: { item: 0 } }
    const call3 = await processSmartCacheV2({
      ...baseParams(context, backend),
      input: [itemB],
    })
    // itemB's data differs from itemA's, so its cache key is different -> miss,
    // exactly as if it were the first time this index had ever been seen.
    expect(call3[0]).toEqual([]) // Cache Hit
    expect(call3[1]).toHaveLength(1) // Cache Miss
    expect(call3[1][0].json).toEqual({ payload: 'B' })
    expect(context[0]?.pending).toBe(true)
  })

  test('batch mode: pending clears per-batch and a later batch reusing the same indices starts fresh', async () => {
    const backend = new InMemoryBackend()
    const context: IContextObject = {}

    const batch1: INodeExecutionData[] = [
      { json: { a: 1 }, pairedItem: { item: 0 } },
      { json: { a: 2 }, pairedItem: { item: 1 } },
    ]
    const call1 = await processSmartCacheV2({
      ...baseParams(context, backend, { batchMode: true }),
      input: batch1,
    })
    expect(call1[0]).toEqual([])
    expect(call1[1]).toHaveLength(2)
    expect(context[0]?.pending).toBe(true)
    expect(context[1]?.pending).toBe(true)

    const writeBack1 = call1[1].map((item) => ({ ...item, json: { ...item.json, done: true } }))
    const call2 = await processSmartCacheV2({
      ...baseParams(context, backend, { batchMode: true }),
      input: writeBack1,
    })
    expect(call2[1]).toEqual([])
    expect(call2[0]).toHaveLength(2)
    expect(context[0]?.pending).toBe(false)
    expect(context[1]?.pending).toBe(false)

    // A later, unrelated batch reuses indices 0 and 1 with different data.
    const batch2: INodeExecutionData[] = [
      { json: { a: 99 }, pairedItem: { item: 0 } },
      { json: { a: 100 }, pairedItem: { item: 1 } },
    ]
    const call3 = await processSmartCacheV2({
      ...baseParams(context, backend, { batchMode: true }),
      input: batch2,
    })
    expect(call3[0]).toEqual([]) // fresh data -> miss, not treated as write-back
    expect(call3[1]).toHaveLength(2)
  })
})

describe('SmartCacheRedisV2 — mixed fresh and write-back items in one call', () => {
  test('a single getInputData(0) array containing both a first-pass item and an already-looped-back item handles both correctly', async () => {
    const backend = new InMemoryBackend()
    const context: IContextObject = {}

    // Prime index 0 as an outstanding write-back (as if it missed on a prior call).
    const primeMiss: INodeExecutionData = { json: { payload: 'first' }, pairedItem: { item: 0 } }
    const primeCall = await processSmartCacheV2({
      ...baseParams(context, backend),
      input: [primeMiss],
    })
    expect(primeCall[1]).toHaveLength(1)
    expect(context[0]?.pending).toBe(true)

    // Now a single call arrives with BOTH: item 0's write-back result AND a
    // brand-new item at index 1 on its first pass.
    const writeBackItem: INodeExecutionData = {
      json: { payload: 'first', processed: true },
      pairedItem: { item: 0 },
    }
    const freshItem: INodeExecutionData = { json: { payload: 'second' }, pairedItem: { item: 1 } }

    const mixedCall = await processSmartCacheV2({
      ...baseParams(context, backend),
      input: [writeBackItem, freshItem],
    })

    // Write-back item 0: persisted, passed through on Cache Hit, pending cleared.
    expect(context[0]?.pending).toBe(false)
    expect(mixedCall[0]).toHaveLength(1)
    expect(mixedCall[0][0]).toEqual(writeBackItem)

    // Fresh item 1: cache is empty for it -> miss, and now marked pending.
    expect(mixedCall[1]).toHaveLength(1)
    expect(mixedCall[1][0].json).toEqual({ payload: 'second' })
    expect(context[1]?.pending).toBe(true)

    // Confirm the write actually landed in the backend (readable as a hit
    // for the same data on a subsequent fresh pass at a different index).
    const rereadItem: INodeExecutionData = { json: { payload: 'first' }, pairedItem: { item: 5 } }
    const rereadCall = await processSmartCacheV2({
      ...baseParams(context, backend),
      input: [rereadItem],
    })
    expect(rereadCall[0]).toHaveLength(1) // Cache Hit
    expect(rereadCall[0][0].pairedItem).toEqual({ item: 5 })
  })

  test('empty input returns [[], []] without touching the backend', async () => {
    const backend = new InMemoryBackend()
    const context: IContextObject = {}

    const result = await processSmartCacheV2({
      ...baseParams(context, backend),
      input: [],
    })

    expect(result).toEqual([[], []])
  })
})
