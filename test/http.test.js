import assert from 'node:assert/strict';
import test from 'node:test';
import { handleError } from '../src/http.js';

const createResponse = () => ({
    statusCode: null,
    body: null,
    status(statusCode) {
        this.statusCode = statusCode;
        return this;
    },
    json(body) {
        this.body = body;
        return this;
    },
});

test("keeps unexpected server error details private", () => {
    const response = createResponse();
    const error = new Error("unexpected database connection failure");

    handleError({ requestId: "request-1" }, response, error);

    assert.equal(response.statusCode, 500);
    assert.deepEqual(response.body, {
        message: "An internal error occurred while processing the request",
        request_id: "request-1",
        error: { status: 500 },
    });
});

test("returns actionable details for explicitly safe downstream failures", () => {
    const response = createResponse();
    const error = new Error("One or more shipments have already been accepted");
    error.statusCode = 502;
    error.expose = true;
    error.code = "SHIPMENT_BATCH_REJECTED";
    error.publicMessage =
        "Shipment submission failed: One or more shipments have already been accepted";
    error.details = {
        summary: { shipments_accepted: 245, rows_skipped: 3 },
        diagnostics: [{
            row_number: 114,
            code: "UNKNOWN_CUSTOMER",
            message: "Customer number is not mapped to an OrderDesk store",
        }],
        downstream: { status: 409 },
    };

    handleError({ requestId: "request-2" }, response, error);

    assert.equal(response.statusCode, 502);
    assert.equal(response.body.message, error.publicMessage);
    assert.equal(response.body.error.code, "SHIPMENT_BATCH_REJECTED");
    assert.deepEqual(response.body.error.details, error.details);
});

test("continues returning validation details for client errors", () => {
    const response = createResponse();
    const error = new Error("Spreadsheet contained no valid shipments");
    error.statusCode = 422;
    error.details = { summary: { shipments_accepted: 0 } };

    handleError({ requestId: "request-3" }, response, error);

    assert.equal(response.statusCode, 422);
    assert.equal(response.body.message, error.message);
    assert.deepEqual(response.body.error.details, error.details);
});
