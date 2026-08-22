/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at https://mozilla.org/MPL/2.0/.
 *
 * Copyright (c) 2026, Rad Orlowski
 */

import { describe, expect, test } from 'vitest'
import type { INodeExecutionData } from 'n8n-workflow'

import { processItemData } from '../shared'

describe('Cache Key Fields dot notation', () => {
  test('resolves a nested field via a dotted path instead of a literal flat key', () => {
    const item: INodeExecutionData = {
      json: { user: { id: 42, name: 'Ada' }, url: 'https://example.com' },
    }

    const result = processItemData(item, 'user.id, url')

    expect(result).toEqual({ 'user.id': 42, url: 'https://example.com' })
  })

  test('a plain (non-dotted) field keeps working exactly as before', () => {
    const item: INodeExecutionData = { json: { id: 1, name: 'plain' } }

    const result = processItemData(item, 'id')

    expect(result).toEqual({ id: 1 })
  })

  test('a missing intermediate segment resolves to undefined rather than throwing', () => {
    const item: INodeExecutionData = { json: { user: null } }

    const result = processItemData(item, 'user.id')

    expect(result).toEqual({ 'user.id': undefined })
  })
})
