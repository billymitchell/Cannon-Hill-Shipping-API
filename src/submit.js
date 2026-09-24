import fetch from 'node-fetch';
import { createHash } from 'crypto';
import { SUBMIT_API_KEY, SUBMIT_ROUTE } from './config.js';
import { log } from './logger.js';

export const buildIdempotencyKey = (data) =>
    `cannon-hill-${createHash("sha256")
        .update(JSON.stringify(data))
        .digest("hex")}`;

export const buildSubmitHeaders = (data) => {
    const headers = {
        "Content-Type": "application/json",
        "Idempotency-Key": buildIdempotencyKey(data),
    };
    if (SUBMIT_API_KEY) {
        headers["X-API-Key"] = SUBMIT_API_KEY;
    }
    return headers;
};

const parseJsonResponse = (rawResponse) => {
    if (!rawResponse) {
        return {};
    }

    try {
        return JSON.parse(rawResponse);
    } catch (error) {
        const parseError = new Error("Downstream API returned invalid JSON");
        parseError.statusCode = 502;
        parseError.retriable = true;
        throw parseError;
    }
};

const isRetriableStatus = (status) =>
    status === 408 || status === 429 || status >= 500;

const simplifyPostResponses = (postResponses) => {
    if (!Array.isArray(postResponses) || postResponses.length === 0) {
        return [];
    }

    return postResponses.map(({ postResponse = {}, error }) => {
        if (error) {
            const errorMessage =
                typeof error === "string"
                    ? error
                    : (error.message || "Downstream shipment processing failed");
            return { status: "error", message: errorMessage };
        }

        return {
            status: postResponse.status || "unknown",
            message: postResponse.message || "No message provided",
        };
    });
};

export const postToSubmitRoute = async (
    data,
    retries = 3,
    requestId = null
) => {
    if (!Array.isArray(data) || data.length === 0) {
        const error = new Error("No valid data to send to submit route");
        error.statusCode = 422;
        throw error;
    }

    log("Submitting shipment batch", {
        event: "shipment_batch_submitting",
        request_id: requestId,
        shipment_count: data.length,
    });

    for (let attempt = 1; attempt <= retries; attempt += 1) {
        try {
            const attemptStartedAt = Date.now();
            const headers = buildSubmitHeaders(data);

            const response = await fetch(SUBMIT_ROUTE, {
                method: 'POST',
                headers,
                body: JSON.stringify(data),
            });
            const rawResponse = await response.text();
            const jsonResponse = parseJsonResponse(rawResponse);

            if (!response.ok) {
                const error = new Error(
                    jsonResponse.message ||
                    `Downstream submission failed with status ${response.status}`
                );
                error.statusCode = 502;
                error.downstreamStatus = response.status;
                error.downstreamErrors = jsonResponse.errors;
                error.retriable = isRetriableStatus(response.status);
                throw error;
            }

            const responseMessage =
                jsonResponse.message || "Shipment batch accepted";
            const isQueued =
                response.status === 202 ||
                String(jsonResponse.status || "").toLowerCase() === "queued" ||
                /\bqueued\b/i.test(responseMessage);
            const simplifiedResults = Array.isArray(jsonResponse.results)
                ? simplifyPostResponses(jsonResponse.results)
                : [];

            log(
                isQueued
                    ? "Shipment batch queued successfully"
                    : "Shipment batch submitted successfully",
                {
                    event: isQueued
                        ? "shipment_batch_queued"
                        : "shipment_batch_submitted",
                    request_id: requestId,
                    shipment_count: data.length,
                    downstream_status_code: response.status,
                    downstream_result_count: simplifiedResults.length,
                    duration_ms: Date.now() - attemptStartedAt,
                }
            );

            return {
                status: isQueued
                    ? "queued"
                    : (jsonResponse.status || "success"),
                message: responseMessage,
                execution_time: jsonResponse.execution_time || "N/A",
                results: simplifiedResults,
                batch_id: jsonResponse.batch_id || null,
                shipment_count:
                    jsonResponse.shipment_count ?? data.length,
                status_url: jsonResponse.status_url || null,
                replayed: jsonResponse.replayed === true,
            };
        } catch (error) {
            const isFinalAttempt =
                attempt === retries || error.retriable === false;
            log(
                isFinalAttempt
                    ? "Shipment batch submission failed"
                    : "Shipment batch submission failed; retrying",
                {
                    event: isFinalAttempt
                        ? "shipment_batch_failed"
                        : "shipment_batch_retry",
                    request_id: requestId,
                    attempt,
                    max_attempts: retries,
                    error,
                },
                isFinalAttempt ? "error" : "warn"
            );

            if (isFinalAttempt) {
                throw error;
            }
            await new Promise((resolve) => setTimeout(resolve, attempt * 1000));
        }
    }
};
