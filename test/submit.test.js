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

test("isolates already accepted shipments and submits the rest", async () => {
    const { postToSubmitRoute } = await import("../src/submit.js");
    const shipments = [
        { source_id: "68125-1" },
        { source_id: "68125-2" },
        { source_id: "68125-3" },
        { source_id: "68125-4" },
    ];
    Object.defineProperty(shipments[0], "__source_row_number", {
        value: 17,
        enumerable: false,
    });

    const submittedBatches = [];
    const fetchImpl = async (_url, options) => {
        const batch = JSON.parse(options.body);
        submittedBatches.push(batch.map((shipment) => shipment.source_id));
        const containsAcceptedShipment = batch.some(
            (shipment) => shipment.source_id === "68125-1"
        );

        return {
            ok: !containsAcceptedShipment,
            status: containsAcceptedShipment ? 409 : 202,
            text: async () => JSON.stringify(
                containsAcceptedShipment
                    ? { message: "One or more shipments have already been accepted" }
                    : {
                        status: "queued",
                        batch_id: `batch-${batch[0].source_id}`,
                        shipment_count: batch.length,
                    }
            ),
        };
    };

    const response = await postToSubmitRoute(
        shipments,
        1,
        "request-duplicate",
        fetchImpl
    );

    assert.equal(response.status, "queued");
    assert.equal(response.shipment_count, 3);
    assert.equal(response.batch_count, 2);
    assert.deepEqual(response.duplicate_shipments, [{
        source_id: "68125-1",
        row_number: 17,
    }]);
    assert.deepEqual(submittedBatches, [
        ["68125-1", "68125-2", "68125-3", "68125-4"],
        ["68125-1", "68125-2"],
        ["68125-1"],
        ["68125-2"],
        ["68125-3", "68125-4"],
    ]);
});

test("does not split unrelated downstream conflicts", async () => {
    const { postToSubmitRoute } = await import("../src/submit.js");
    let requestCount = 0;
    const fetchImpl = async () => {
        requestCount += 1;
        return {
            ok: false,
            status: 409,
            text: async () => JSON.stringify({
                message: "Idempotency key was reused with different content",
            }),
        };
    };

    await assert.rejects(
        postToSubmitRoute(
            [{ source_id: "68125-1" }, { source_id: "68125-2" }],
            1,
            "request-conflict",
            fetchImpl
        ),
        /Idempotency key was reused/
    );
    assert.equal(requestCount, 1);
});
