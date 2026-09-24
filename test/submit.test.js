import assert from 'node:assert/strict';
import test from 'node:test';

test("builds the documented authenticated, idempotent batch headers", async () => {
    process.env.SUBMIT_API_KEY = "test-inbound-api-key";

    try {
        const { buildIdempotencyKey, buildSubmitHeaders } =
            await import("../src/submit.js");
        const shipments = [{
            source_id: "68125-123456",
            tracking_number: "1Z999AA10123456784",
            carrier_code: "ups",
            shipment_method: "Ground",
        }];

        const headers = buildSubmitHeaders(shipments);

        assert.equal(headers["Content-Type"], "application/json");
        assert.equal(headers["X-API-Key"], "test-inbound-api-key");
        assert.match(
            headers["Idempotency-Key"],
            /^cannon-hill-[a-f0-9]{64}$/
        );
        assert.equal(
            headers["Idempotency-Key"],
            buildIdempotencyKey(shipments)
        );
    } finally {
        delete process.env.SUBMIT_API_KEY;
    }
});
