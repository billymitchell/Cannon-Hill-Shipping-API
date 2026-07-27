import { randomUUID } from 'crypto';
import { log } from './logger.js';

export const requestLogger = (req, res, next) => {
    const requestId = req.headers["x-request-id"] || randomUUID();
    const startTime = Date.now();

    req.requestId = requestId;
    res.setHeader("x-request-id", requestId);

    log("HTTP request started", {
        event: "http_request_started",
        request_id: requestId,
        method: req.method,
        path: req.originalUrl || req.url,
        content_type: req.headers["content-type"],
    });

    res.on("finish", () => {
        log("HTTP request finished", {
            event: "http_request_finished",
            request_id: requestId,
            method: req.method,
            path: req.originalUrl || req.url,
            status_code: res.statusCode,
            duration_ms: Date.now() - startTime,
        });
    });

    next();
};

export const handleError = (request, response, error) => {
    const statusCode = error?.statusCode || error?.status || 500;
    const requestId = request?.requestId;

    log("Handling error", {
        event: "request_failed",
        request_id: requestId,
        status_code: statusCode,
        error,
    }, "error");

    const userMessage =
        statusCode >= 500
            ? "An internal error occurred while processing the request"
            : (error?.message || "Request failed");
    const details =
        statusCode < 500
            ? (error?.details || error?.message || "Request failed")
            : undefined;

    response.status(statusCode).json({
        message: userMessage,
        request_id: requestId,
        error: {
            status: statusCode,
            ...(details ? { details } : {}),
        },
    });
};
