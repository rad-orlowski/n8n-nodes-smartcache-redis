/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2025, Victor Duarte
 * Copyright (c) 2026, Rad Orlowski — Redis backend adaptation
 *
 * Adapted from n8n-nodes-smartcache (https://github.com/skadaai/n8n-nodes-smartcache)
 * S3Backend removed; RedisBackend lives in redisStorage.ts.
 */

export interface CacheBackend {
  head(key: string): Promise<{ lastModified: Date } | null>
  get<T = unknown>(key: string): Promise<T | null>
  put<T = unknown>(key: string, value: T): Promise<void>
}

export const joinPrefix = (prefix: string, key: string) => {
  const cleanPrefix = (prefix || '').replace(/^\/+|\/+$/g, '')
  return cleanPrefix ? `${cleanPrefix}/${key}` : key
}
