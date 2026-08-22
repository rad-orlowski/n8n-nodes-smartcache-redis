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
 * Versioned dispatcher: n8n resolves typeVersion 1 to the original dual-input
 * design (SmartCacheRedisV1, unchanged — existing production workflows are
 * pinned to it and keep running exactly as they do today) and typeVersion 2
 * to the fixed single-input design (SmartCacheRedisV2). New/opted-in
 * workflows get V2 via the node's version selector in the canvas.
 */

import { VersionedNodeType, type INodeTypeBaseDescription } from 'n8n-workflow'

import { SmartCacheRedisV1 } from './SmartCacheRedisV1.node'
import { SmartCacheRedisV2 } from './SmartCacheRedisV2.node'

export class SmartCacheRedis extends VersionedNodeType {
  constructor() {
    const baseDescription: INodeTypeBaseDescription = {
      displayName: 'Smart Cache (Redis)',
      name: 'smartCacheRedis',
      icon: 'file:smartCacheRedis.svg',
      group: ['transform'],
      description:
        'Intelligent caching node with automatic hash generation and TTL support. Persists cache entries to Redis with native key expiry.',
      documentationUrl: 'https://github.com/rad-orlowski/n8n-nodes-smartcache-redis#readme',
      defaultVersion: 2,
    }

    const nodeVersions = {
      1: new SmartCacheRedisV1(),
      2: new SmartCacheRedisV2(),
    }

    super(nodeVersions, baseDescription)
  }
}
