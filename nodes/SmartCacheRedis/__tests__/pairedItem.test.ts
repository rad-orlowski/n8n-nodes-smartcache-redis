/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2026, Rad Orlowski
 */

import { describe, expect, test } from 'vitest'
import type { IContextObject, INodeExecutionData } from 'n8n-workflow'

import type { CacheBackend } from '../storage'
import { processBatch, processSingleItem, writeToCache } from '../SmartCacheRedis.node'

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

describe('cache hit pairedItem (single-item mode)', () => {
  test('a hit re-stamps pairedItem to the CURRENT item index, not the write-time index', async () => {
    const backend = new InMemoryBackend()
    const nodeId = 'node-1'
    const payload = { url: 'https://example.com' }

    // --- Run 1: write ---
    const writeContext: IContextObject = {}
    const writeItem: INodeExecutionData = { json: payload, pairedItem: { item: 0 } }
    const { miss } = await processSingleItem(
      writeItem,
      writeContext,
      '',
      'smartcache',
      false,
      0,
      noopLogger,
      nodeId,
      backend,
    )
    expect(miss).not.toBeNull()
    await writeToCache(miss!, writeContext, backend)

    // --- Run 2: same data, but this execution's item sits at a DIFFERENT input index ---
    const readContext: IContextObject = {}
    const readItem: INodeExecutionData = { json: payload, pairedItem: { item: 2 } }
    const { hit } = await processSingleItem(
      readItem,
      readContext,
      '',
      'smartcache',
      false,
      0,
      noopLogger,
      nodeId,
      backend,
    )

    expect(hit).not.toBeNull()
    expect(hit!.pairedItem).toEqual({ item: 2 })
  })
})

describe('cache hit pairedItem (batch mode)', () => {
  test('a hit re-stamps every returned item to the CURRENT batch indices', async () => {
    const backend = new InMemoryBackend()
    const nodeId = 'node-1'
    const items = [{ a: 1 }, { a: 2 }]

    // --- Run 1: write, items paired to indices 0 and 1 ---
    const writeContext: IContextObject = {}
    const writeItems: INodeExecutionData[] = items.map((json, i) => ({
      json,
      pairedItem: { item: i },
    }))
    const { misses } = await processBatch(
      writeItems,
      writeContext,
      '',
      'smartcache',
      false,
      0,
      noopLogger,
      nodeId,
      backend,
    )
    await writeToCache(misses, writeContext, backend)

    // --- Run 2: same data, but this execution's items sit at indices 5 and 6 ---
    const readContext: IContextObject = {}
    const readItems: INodeExecutionData[] = items.map((json, i) => ({
      json,
      pairedItem: { item: i + 5 },
    }))
    const { hits } = await processBatch(
      readItems,
      readContext,
      '',
      'smartcache',
      false,
      0,
      noopLogger,
      nodeId,
      backend,
    )

    expect(hits).toHaveLength(2)
    expect(hits.map((h) => h.pairedItem)).toEqual([{ item: 5 }, { item: 6 }])
  })
})
