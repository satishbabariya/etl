import { describe, it, expect } from 'vitest';
import { buildListCustomers } from '../src/request.js';

describe('buildListCustomers', () => {
    it('first page url has no starting_after', () => {
        const r = buildListCustomers('sk_test_x', 100, undefined, 'https://api.stripe.com');
        expect(r.url).toBe('https://api.stripe.com/v1/customers?limit=100');
    });

    it('paginated url includes starting_after', () => {
        const r = buildListCustomers('sk_test_x', 50, 'cus_42', 'https://api.stripe.com');
        expect(r.url).toBe(
            'https://api.stripe.com/v1/customers?limit=50&starting_after=cus_42',
        );
    });

    it('auth header uses bearer', () => {
        const r = buildListCustomers('sk_test_secret', 1, undefined, 'https://api.stripe.com');
        expect(
            r.headers.find(([k, v]) => k === 'Authorization' && v === 'Bearer sk_test_secret'),
        ).toBeDefined();
    });

    it('stripe-version pinned', () => {
        const r = buildListCustomers('k', 1, undefined, 'https://api.stripe.com');
        expect(
            r.headers.find(([k, v]) => k === 'Stripe-Version' && v === '2024-04-10'),
        ).toBeDefined();
    });
});
