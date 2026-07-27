import fetch from 'node-fetch';
import { SUBMIT_ROUTE } from './config.js';
import { log } from './logger.js';

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
            const response = await fetch(SUBMIT_ROUTE, {
                method: 'POST',
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(data),
            });
            const rawResponse = await response.text();

            if (!response.ok) {
                const error = new Error(
                    `Downstream submission failed with status ${response.status}`
                );
                error.statusCode = 502;
                error.downstreamStatus = response.status;
                throw error;
            }

            const jsonResponse = JSON.parse(rawResponse);
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
            };
        } catch (error) {
            const isFinalAttempt = attempt === retries;
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
