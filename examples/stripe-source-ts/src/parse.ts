// Pure JSON → Arrow IPC parser for Stripe /v1/customers responses.
// Lives outside connector.ts so vitest can exercise it in plain Node.

import {
    Field,
    Schema,
    Int64,
    Utf8,
    tableFromArrays,
    tableToIPC,
} from 'apache-arrow';

export interface Customer {
    id: string;
    email: string | null;
    name: string | null;
    created: number;
}

export interface ListResp {
    data: Customer[];
    has_more: boolean;
}

export function customersSchema(): Schema {
    return new Schema([
        new Field('id', new Utf8(), false),
        new Field('email', new Utf8(), true),
        new Field('name', new Utf8(), true),
        new Field('created', new Int64(), false),
    ]);
}

export function schemaIpcBytes(): Uint8Array {
    const t = tableFromArrays({
        id: [] as string[],
        email: [] as (string | null)[],
        name: [] as (string | null)[],
        created: BigInt64Array.from([]),
    });
    return tableToIPC(t, 'stream');
}

export interface ParsedPage {
    batchIpc: Uint8Array;
    rows: number;
    lastId: string | undefined;
    hasMore: boolean;
}

export function parsePage(jsonBytes: Uint8Array): ParsedPage {
    const text = new TextDecoder().decode(jsonBytes);
    const resp = JSON.parse(text) as ListResp;
    if (!Array.isArray(resp.data)) {
        throw new Error('stripe response missing `data` array');
    }
    const ids = resp.data.map((c) => c.id);
    const emails = resp.data.map((c) => c.email ?? null);
    const names = resp.data.map((c) => c.name ?? null);
    const createds = BigInt64Array.from(resp.data.map((c) => BigInt(c.created)));
    const t = tableFromArrays({
        id: ids,
        email: emails,
        name: names,
        created: createds,
    });
    return {
        batchIpc: tableToIPC(t, 'stream'),
        rows: resp.data.length,
        lastId: resp.data.length > 0 ? resp.data[resp.data.length - 1].id : undefined,
        hasMore: !!resp.has_more,
    };
}
