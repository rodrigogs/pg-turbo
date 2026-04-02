// ts/tests/unit/connection.test.ts
import { describe, expect, it } from 'vitest'
import {
  appendKeepaliveParams,
  cleanConnectionString,
  extractDbName,
  sanitizeConnectionString,
} from '../../src/core/connection.js'

describe('sanitizeConnectionString', () => {
  it('masks password', () => {
    expect(sanitizeConnectionString('postgresql://user:secret@host/db')).toBe('postgresql://user:***@host/db')
  })
  it('handles no password', () => {
    expect(sanitizeConnectionString('postgresql://user@host/db')).toBe('postgresql://user@host/db')
  })
  it('strips query params', () => {
    expect(sanitizeConnectionString('postgresql://user:pass@host/db?sslmode=require')).toBe(
      'postgresql://user:***@host/db',
    )
  })
})

describe('extractDbName', () => {
  it('extracts database name', () => {
    expect(extractDbName('postgresql://user:pass@host/mydb')).toBe('mydb')
  })
  it('strips query params', () => {
    expect(extractDbName('postgresql://user:pass@host/mydb?sslmode=require')).toBe('mydb')
  })
  it('handles port', () => {
    expect(extractDbName('postgresql://user:pass@host:5432/mydb')).toBe('mydb')
  })
})

describe('cleanConnectionString', () => {
  it('strips GUI params but keeps sslmode', () => {
    expect(cleanConnectionString('postgresql://u:p@h/db?statusColor=red&sslmode=require&env=staging')).toBe(
      'postgresql://u:p@h/db?sslmode=require',
    )
  })
  it('strips all params when no sslmode', () => {
    expect(cleanConnectionString('postgresql://u:p@h/db?statusColor=red&env=staging')).toBe('postgresql://u:p@h/db')
  })
  it('returns as-is when no query params', () => {
    expect(cleanConnectionString('postgresql://u:p@h/db')).toBe('postgresql://u:p@h/db')
  })
})

describe('appendKeepaliveParams', () => {
  it('appends keepalive params to clean URL', () => {
    const result = appendKeepaliveParams('postgresql://u:p@h/db')
    expect(result).toContain('keepalives=1')
    expect(result).toContain('keepalives_idle=10')
    expect(result).toContain('connect_timeout=10')
  })
  it('appends to URL with existing params', () => {
    const result = appendKeepaliveParams('postgresql://u:p@h/db?sslmode=require')
    expect(result).toContain('sslmode=require')
    expect(result).toContain('keepalives=1')
  })
  it('does not override existing keepalive params', () => {
    const result = appendKeepaliveParams('postgresql://u:p@h/db?keepalives_idle=60&connect_timeout=30')
    expect(result).toContain('keepalives_idle=60') // preserved
    expect(result).toContain('connect_timeout=30') // preserved
    expect(result).not.toMatch(/keepalives_idle=10/) // not overridden
    expect(result).not.toMatch(/connect_timeout=10/) // not overridden
    expect(result).toContain('keepalives=1') // added (wasn't present)
  })
})
