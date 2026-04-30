// Pure JSON → Arrow IPC parser for Stripe /v1/customers responses.
// Lives outside connector.ts so vitest can exercise it in plain Node.

import {
    Field,
    Schema,
    Int64,
    Utf8,
    Table,
    tableToIPC,
    vectorFromArray,
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

// Build a Table whose vectors carry explicit Utf8/Int64 types.
// `tableFromArrays` infers Dictionary<Int32, Utf8> for strings, which
// the Rust StreamReader treats as a different DataType than Utf8.
function tableFromCustomers(customers: Customer[]): Table {
    const id = vectorFromArray(
        customers.map((c) => c.id),
        new Utf8(),
    );
    const email = vectorFromArray(
        customers.map((c) => c.email ?? ''),
        new Utf8(),
    );
    const name = vectorFromArray(
        customers.map((c) => c.name ?? ''),
        new Utf8(),
    );
    const created = vectorFromArray(
        BigInt64Array.from(customers.map((c) => BigInt(c.created))),
    );
    return new Table({ id, email, name, created });
}

export function schemaIpcBytes(): Uint8Array {
    // One placeholder row so apache-arrow infers concrete types
    // (`tableFromArrays`/`new Table` collapse empty arrays to Null).
    // The worker calls `StreamReader::try_new(schema_bytes).schema()`,
    // discarding any data — the placeholder is invisible downstream.
    const t = tableFromCustomers([{ id: '', email: '', name: '', created: 0 }]);
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
    const t = tableFromCustomers(resp.data);
    return {
        batchIpc: tableToIPC(t, 'stream'),
        rows: resp.data.length,
        lastId: resp.data.length > 0 ? resp.data[resp.data.length - 1].id : undefined,
        hasMore: !!resp.has_more,
    };
}
