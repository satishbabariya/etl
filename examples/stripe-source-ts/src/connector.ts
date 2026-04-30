// stripe-source-ts — Stripe /v1/customers as an ETL source (TS port).
//
// jco wraps export return values: returning `bytes` becomes
// `result.ok = bytes`, throwing becomes `result.err = e`. Do NOT
// return `{ tag: 'ok', val: ... }` — that double-wraps and the host
// sees a 0-byte payload.

// @ts-expect-error - resolved by jco at componentize time
import { log, httpFetch } from 'platform:connector/host@0.1.0';

import { parsePage, schemaIpcBytes } from './parse.js';
import { buildListCustomers } from './request.js';

interface StripeSourceCfg {
    base_url: string;
    limit: number;
    max_429_retries: number;
}

function defaultCfg(): StripeSourceCfg {
    return { base_url: 'https://api.stripe.com', limit: 100, max_429_retries: 3 };
}

function parseSourceCfg(json: string): StripeSourceCfg {
    const d = defaultCfg();
    if (!json || json.trim() === '') return d;
    try {
        const parsed = JSON.parse(json);
        return {
            base_url: typeof parsed.base_url === 'string' ? parsed.base_url : d.base_url,
            limit: typeof parsed.limit === 'number' ? parsed.limit : d.limit,
            max_429_retries:
                typeof parsed.max_429_retries === 'number'
                    ? parsed.max_429_retries
                    : d.max_429_retries,
        };
    } catch {
        return d;
    }
}

interface HttpResponse {
    status: number;
    headers: [string, string][];
    body: Uint8Array;
}

function fetchWithRetry(
    method: string,
    url: string,
    headers: [string, string][],
    maxRetries: number,
): Uint8Array {
    let attempt = 0;
    // eslint-disable-next-line no-constant-condition
    while (true) {
        const resp = httpFetch({ method, url, headers, body: undefined }) as HttpResponse;
        if (resp.status === 429 && attempt < maxRetries) {
            log('warn', `stripe-source-ts: 429 retry ${attempt + 1}/${maxRetries}`);
            attempt += 1;
            continue;
        }
        if (resp.status >= 200 && resp.status < 300) return resp.body;
        const bodyText = new TextDecoder().decode(resp.body);
        throw new Error(`stripe HTTP ${resp.status}: ${bodyText}`);
    }
}

export const discover = (
    _conn: { url: string },
    _source: { json: string },
): Uint8Array => {
    return schemaIpcBytes();
};

export const readBatch = (
    conn: { url: string },
    source: { json: string },
    cursor: { kind: 'int64' | 'timestamp-tz'; value: string } | undefined,
    _batchSize: number,
): {
    batchIpc: Uint8Array;
    rows: number;
    newCursor: { kind: 'int64' | 'timestamp-tz'; value: string } | undefined;
    isFinal: boolean;
} => {
    const cfg = parseSourceCfg(source.json);
    const startingAfter = cursor?.value;
    const req = buildListCustomers(conn.url, cfg.limit, startingAfter, cfg.base_url);
    const body = fetchWithRetry('GET', req.url, req.headers, cfg.max_429_retries);
    const page = parsePage(body);
    const newCursor = page.lastId
        ? { kind: 'int64' as const, value: page.lastId }
        : undefined;
    return {
        batchIpc: page.batchIpc,
        rows: page.rows,
        newCursor,
        isFinal: !page.hasMore,
    };
};
