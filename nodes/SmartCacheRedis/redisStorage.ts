/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2025, Victor Duarte
 * Copyright (c) 2026, Rad Orlowski — Redis backend adaptation
 *
 * Adapted from n8n-nodes-smartcache (https://github.com/skadaai/n8n-nodes-smartcache)
 * Storage backend replaced: S3 → Redis (native TTL via SET EX).
 */

import Redis from 'ioredis'
import type { CacheBackend } from './storage'

export type RedisCredentials = {
  host: string
  port: number
  database: number
  password?: string
  user?: string
  ssl: boolean
  disableTlsVerification: boolean
}

/**
 * Redis-backed cache backend. Values are stored as a JSON envelope
 * { data, ts } so head() can report age without a second command, and
 * native Redis key TTL (SET ... EX) evicts expired entries automatically —
 * dead keys never accumulate in maxmemory.
 */
export class RedisBackend implements CacheBackend {
  private client: Redis
  private ttlSeconds: number

  constructor(creds: RedisCredentials, ttlHours: number) {
    const options: Record<string, unknown> = {
      host: creds.host || 'localhost',
      port: creds.port || 6379,
      db: creds.database ?? 0,
      // ioredis treats empty-string user/password as "no auth", so only pass when set
      ...(creds.password ? { password: creds.password } : {}),
      ...(creds.user && creds.password ? { username: creds.user } : {}),
      lazyConnect: true,
      maxRetriesPerRequest: 1,
      enableOfflineQueue: false,
      connectTimeout: 10_000,
      commandTimeout: 30_000,
      ...(creds.ssl
        ? {
            tls: creds.disableTlsVerification
              ? { rejectUnauthorized: false }
              : {},
          }
        : {}),
    }
    this.client = new Redis(options)
    this.ttlSeconds = ttlHours > 0 ? Math.round(ttlHours * 3600) : 0
  }

  async ensureConnection(): Promise<void> {
    if (this.client.status !== 'ready') {
      await this.client.connect()
    }
  }

  async head(key: string): Promise<{ lastModified: Date } | null> {
    const raw = await this.client.get(key)
    if (raw == null) return null
    try {
      const envelope = JSON.parse(raw) as { ts: number }
      return { lastModified: new Date(envelope.ts) }
    } catch {
      // Key exists but isn't ours — treat as present with unknown age
      return { lastModified: new Date(0) }
    }
  }

  async get<T = unknown>(key: string): Promise<T | null> {
    const raw = await this.client.get(key)
    if (raw == null) return null
    try {
      const envelope = JSON.parse(raw) as { data: T; ts: number }
      return envelope.data
    } catch {
      return null
    }
  }

  async put<T = unknown>(key: string, value: T): Promise<void> {
    const envelope = { data: value, ts: Date.now() }
    const body = JSON.stringify(envelope)
    if (this.ttlSeconds > 0) {
      await this.client.set(key, body, 'EX', this.ttlSeconds)
    } else {
      await this.client.set(key, body)
    }
  }

  async quit(): Promise<void> {
    if (this.client.status !== 'end') {
      await this.client.quit().catch(() => this.client.disconnect())
    }
  }
}

export { joinPrefix } from './storage'
