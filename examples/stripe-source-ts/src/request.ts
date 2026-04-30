// HTTP request builder for Stripe /v1/customers — pure, no I/O.

export interface StripeRequest {
    url: string;
    headers: [string, string][];
}

export function buildListCustomers(
    apiKey: string,
    limit: number,
    startingAfter: string | undefined,
    baseUrl: string,
): StripeRequest {
    let url = `${baseUrl}/v1/customers?limit=${limit}`;
    if (startingAfter !== undefined) {
        url += `&starting_after=${startingAfter}`;
    }
    return {
        url,
        headers: [
            ['Authorization', `Bearer ${apiKey}`],
            ['Stripe-Version', '2024-04-10'],
        ],
    };
}
