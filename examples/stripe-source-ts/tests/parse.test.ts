import { describe, it, expect } from 'vitest';
import { customersSchema, parsePage, schemaIpcBytes } from '../src/parse.js';

describe('customersSchema', () => {
    it('has 4 columns in canonical order', () => {
        const s = customersSchema();
        expect(s.fields.map((f) => f.name)).toEqual(['id', 'email', 'name', 'created']);
        expect(s.fields[0].nullable).toBe(false);
        expect(s.fields[1].nullable).toBe(true);
    });
});

describe('schemaIpcBytes', () => {
    it('produces a non-empty IPC stream', () => {
        const b = schemaIpcBytes();
        expect(b.byteLength).toBeGreaterThan(0);
    });
});

describe('parsePage', () => {
    it('parses two customers', () => {
        const json = `{"data":[
            {"id":"cus_a","email":"a@x.com","name":"Alice","created":1700000000},
            {"id":"cus_b","email":"b@x.com","name":"Bob","created":1700000123}
        ],"has_more":false}`;
        const p = parsePage(new TextEncoder().encode(json));
        expect(p.rows).toBe(2);
        expect(p.lastId).toBe('cus_b');
        expect(p.hasMore).toBe(false);
        expect(p.batchIpc.byteLength).toBeGreaterThan(0);
    });

    it('parses an empty page', () => {
        const json = `{"data":[],"has_more":false}`;
        const p = parsePage(new TextEncoder().encode(json));
        expect(p.rows).toBe(0);
        expect(p.lastId).toBeUndefined();
        expect(p.hasMore).toBe(false);
    });

    it('rejects malformed JSON', () => {
        expect(() => parsePage(new TextEncoder().encode('{not json'))).toThrow();
    });
});
