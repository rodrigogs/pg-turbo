// ts/tests/unit/copy-stream.test.ts
import { describe, it, expect } from 'vitest'
import { buildRestoreCopyQuery, chunkDoneMarker, chunkRestoredMarker } from '../../src/core/copy-stream.js'

describe('buildRestoreCopyQuery', () => {
  it('builds COPY FROM query with columns', () => {
    expect(buildRestoreCopyQuery('public', 'users', ['id', 'name', 'email']))
      .toBe('COPY "public"."users" ("id", "name", "email") FROM STDIN')
  })
  it('quotes schema and table names', () => {
    expect(buildRestoreCopyQuery('my schema', 'my-table', ['col']))
      .toBe('COPY "my schema"."my-table" ("col") FROM STDIN')
  })
})

describe('marker helpers', () => {
  it('generates dump done marker path', () => {
    expect(chunkDoneMarker('/out/data/public.users/chunk_0000.copy.lz4'))
      .toBe('/out/data/public.users/chunk_0000.copy.lz4.done')
  })
  it('generates restore done marker path', () => {
    expect(chunkRestoredMarker('/out/data/public.users/chunk_0000.copy.lz4'))
      .toBe('/out/data/public.users/chunk_0000.copy.lz4.restored.done')
  })
})
