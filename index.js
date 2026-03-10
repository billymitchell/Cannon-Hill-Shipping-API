import express from 'express';
import bodyParser from 'body-parser';
import multer from 'multer';
import fetch from 'node-fetch';
import ExcelJS from 'exceljs';
import { randomUUID } from 'crypto';
import 'dotenv/config';

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

const XLSM_MIME_TYPES = new Set([
    "application/vnd.ms-excel.sheet.macroenabled.12",
    "application/octet-stream",
]);

const parsedMaxAttachmentSizeMb = Number.parseInt(process.env.MAX_ATTACHMENT_SIZE_MB || "10", 10);
const MAX_ATTACHMENT_SIZE_MB = Number.isFinite(parsedMaxAttachmentSizeMb) && parsedMaxAttachmentSizeMb > 0
    ? parsedMaxAttachmentSizeMb
    : 10;
const MAX_ATTACHMENT_SIZE_BYTES = MAX_ATTACHMENT_SIZE_MB * 1024 * 1024;
const MAILGUN_API_KEY = process.env.MAILGUN_API_KEY || "";

const isXLSMAttachment = (file = {}) => {
    const fieldName = (file.fieldname || "").toLowerCase();
    const fileName = (file.originalname || "").toLowerCase();
    const mimeType = (file.mimetype || "").toLowerCase();
    const isAttachmentField = fieldName === "file" || fieldName.startsWith("attachment-");

    return (
        isAttachmentField &&
        (
            fileName.endsWith(".xlsm") ||
            XLSM_MIME_TYPES.has(mimeType)
        )
    );
};

const parseMailgunAttachmentsField = (rawAttachments) => {
    if (!rawAttachments) {
        return [];
    }

    if (Array.isArray(rawAttachments)) {
        return rawAttachments;
    }

    if (typeof rawAttachments !== "string") {
        return [];
    }

    try {
        const parsed = JSON.parse(rawAttachments);
        return Array.isArray(parsed) ? parsed : [];
    } catch (error) {
        log("Failed to parse Mailgun attachments metadata", { rawAttachments, error }, "error");
        return [];
    }
};

const extractXLSMMailgunAttachmentMeta = (req) => {
    const attachments = parseMailgunAttachmentsField(req.body?.attachments);

    return attachments.find((attachment = {}) => {
        const fileName = String(attachment.name || "").toLowerCase();
        const mimeType = String(attachment["content-type"] || attachment.contentType || "").toLowerCase();

        return fileName.endsWith(".xlsm") || XLSM_MIME_TYPES.has(mimeType);
    }) || null;
};

const decodeMimeHeaderValue = (value = "") =>
    String(value)
        .replace(/\r?\n[ \t]+/g, " ")
        .trim()
        .replace(/^"(.*)"$/, "$1");

const parseMimeHeaders = (rawHeaders = "") => rawHeaders
    .split(/\r?\n/)
    .reduce((headers, line) => {
        if (!line.trim()) {
            return headers;
        }

        const separatorIndex = line.indexOf(":");
        if (separatorIndex === -1) {
            return headers;
        }

        const key = line.slice(0, separatorIndex).trim().toLowerCase();
        const value = decodeMimeHeaderValue(line.slice(separatorIndex + 1));
        headers[key] = value;
        return headers;
    }, {});

const splitMimeParts = (body = "", boundary = "") => {
    if (!boundary) {
        return [];
    }

    const normalizedBoundary = boundary.replace(/^"(.*)"$/, "$1");
    const segments = body.split(`--${normalizedBoundary}`);

    return segments
        .slice(1)
        .map((segment) => segment.replace(/^\r?\n/, "").replace(/\r?\n--\s*$/, "").trim())
        .filter((segment) => segment && segment !== "--");
};

const parseMimePart = (rawPart = "") => {
    const separator = rawPart.indexOf("\r\n\r\n") >= 0 ? "\r\n\r\n" : "\n\n";
    const separatorIndex = rawPart.indexOf(separator);

    if (separatorIndex === -1) {
        return { headers: {}, body: rawPart };
    }

    return {
        headers: parseMimeHeaders(rawPart.slice(0, separatorIndex)),
        body: rawPart.slice(separatorIndex + separator.length),
    };
};

const extractBoundary = (contentType = "") => {
    const match = /boundary="?([^";]+)"?/i.exec(contentType);
    return match?.[1] || null;
};

const extractMimeFilename = (contentDisposition = "", contentType = "") => {
    const dispositionMatch = /filename\*?="?([^";]+)"?/i.exec(contentDisposition);
    if (dispositionMatch?.[1]) {
        return dispositionMatch[1];
    }

    const typeMatch = /name="?([^";]+)"?/i.exec(contentType);
    return typeMatch?.[1] || "";
};

const decodeMimeBody = (body = "", transferEncoding = "") => {
    const normalizedEncoding = String(transferEncoding || "").toLowerCase();
    const normalizedBody = body.replace(/\r?\n/g, "");

    if (normalizedEncoding === "base64") {
        return Buffer.from(normalizedBody, "base64");
    }

    return Buffer.from(body, "utf8");
};

const extractXLSMBufferFromMime = (rawMime = "", attachmentMeta = {}) => {
    const separator = rawMime.indexOf("\r\n\r\n") >= 0 ? "\r\n\r\n" : "\n\n";
    const separatorIndex = rawMime.indexOf(separator);
    if (separatorIndex === -1) {
        const error = new Error("Stored Mailgun message did not include MIME headers");
        error.statusCode = 502;
        throw error;
    }

    const rootHeaders = parseMimeHeaders(rawMime.slice(0, separatorIndex));
    const rootBody = rawMime.slice(separatorIndex + separator.length);
    const boundary = extractBoundary(rootHeaders["content-type"]);

    if (!boundary) {
        const error = new Error("Stored Mailgun message was not multipart");
        error.statusCode = 502;
        throw error;
    }

    const targetFileName = String(attachmentMeta.name || "").toLowerCase();
    const parts = splitMimeParts(rootBody, boundary);

    for (const rawPart of parts) {
        const { headers, body } = parseMimePart(rawPart);
        const filename = extractMimeFilename(headers["content-disposition"], headers["content-type"]).toLowerCase();
        const mimetype = String(headers["content-type"] || "").split(";")[0].trim().toLowerCase();

        if (filename === targetFileName || filename.endsWith(".xlsm") || XLSM_MIME_TYPES.has(mimetype)) {
            const buffer = decodeMimeBody(body, headers["content-transfer-encoding"]);

            if (buffer.length > MAX_ATTACHMENT_SIZE_BYTES) {
                const error = new Error(`Attachment too large. Max allowed size is ${MAX_ATTACHMENT_SIZE_MB}MB`);
                error.statusCode = 413;
                throw error;
            }

            return {
                fieldname: "mailgun-message-url",
                originalname: filename || attachmentMeta.name || "mailgun-attachment.xlsm",
                mimetype: mimetype || attachmentMeta["content-type"] || "application/octet-stream",
                size: buffer.length,
                buffer,
            };
        }
    }

    const error = new Error("No .xlsm attachment found in stored Mailgun message");
    error.statusCode = 502;
    throw error;
};

const fetchMailgunAttachment = async (req, attachmentMeta) => {
    const messageUrl = req.body?.["message-url"];

    if (!messageUrl) {
        const error = new Error("Mailgun webhook did not include message-url for stored message retrieval");
        error.statusCode = 400;
        throw error;
    }

    if (!MAILGUN_API_KEY) {
        const error = new Error("MAILGUN_API_KEY is required to download Mailgun-hosted attachments");
        error.statusCode = 500;
        throw error;
    }

    const response = await fetch(messageUrl, {
        headers: {
            Authorization: `Basic ${Buffer.from(`api:${MAILGUN_API_KEY}`).toString("base64")}`,
        },
    });

    if (!response.ok) {
        const error = new Error(`Failed to retrieve stored Mailgun message (${response.status})`);
        error.statusCode = 502;
        error.response = await response.text();
        throw error;
    }

    const rawResponse = await response.text();
    let rawMime = rawResponse;

    try {
        const parsedResponse = JSON.parse(rawResponse);
        rawMime = parsedResponse["body-mime"] || parsedResponse["message"] || "";
    } catch (error) {
        // The endpoint can also return a raw MIME message body.
    }

    if (!rawMime) {
        const error = new Error("Stored Mailgun message response did not contain a MIME payload");
        error.statusCode = 502;
        throw error;
    }

    return extractXLSMBufferFromMime(rawMime, attachmentMeta);
};

const upload = multer({
    storage: multer.memoryStorage(),
    limits: {
        fileSize: MAX_ATTACHMENT_SIZE_BYTES,
        files: 20,
    },
    fileFilter: (req, file, cb) => {
        if (isXLSMAttachment(file)) {
            return cb(null, true);
        }

        // Ignore non-.xlsm attachments instead of buffering them.
        cb(null, false);
    },
});

// Utility Functions

/**
 * Logs messages to the console with optional data and log level.
 * Adds more context if available (e.g., error stack).
 * @param {string} message - The message to log.
 * @param {any} data - Additional data to log (optional).
 * @param {string} level - Log level ("info" or "error").
 */
const sanitizeForLogging = (value, seen = new WeakSet()) => {
    if (value instanceof Error) {
        return {
            name: value.name,
            message: value.message,
            stack: value.stack,
        };
    }

    if (typeof value === "string" && value.length > 4000) {
        return `${value.slice(0, 4000)}...<truncated>`;
    }

    if (typeof value === "bigint") {
        return value.toString();
    }

    if (Array.isArray(value)) {
        return value.map((item) => sanitizeForLogging(item, seen));
    }

    if (value && typeof value === "object") {
        if (seen.has(value)) {
            return "[Circular]";
        }
        seen.add(value);

        const result = {};
        Object.entries(value).forEach(([key, val]) => {
            result[key] = sanitizeForLogging(val, seen);
        });
        return result;
    }

    return value;
};

const log = (message, data = null, level = "info") => {
    const levels = { info: console.log, error: console.error };
    const logger = levels[level] || console.log;
    const entry = {
        timestamp: new Date().toISOString(),
        level,
        message,
    };

    if (data !== null && data !== undefined) {
        entry.data = sanitizeForLogging(data);
    }

    logger(JSON.stringify(entry));
};

app.use((req, res, next) => {
    const requestId = req.headers["x-request-id"] || randomUUID();
    const startTime = Date.now();

    req.requestId = requestId;
    res.setHeader("x-request-id", requestId);

    log("HTTP request started", {
        request_id: requestId,
        method: req.method,
        path: req.originalUrl || req.url,
        content_type: req.headers["content-type"],
        user_agent: req.headers["user-agent"],
    });

    res.on("finish", () => {
        log("HTTP request finished", {
            request_id: requestId,
            method: req.method,
            path: req.originalUrl || req.url,
            status_code: res.statusCode,
            duration_ms: Date.now() - startTime,
        });
    });

    next();
});

/**
 * Handles errors by logging them and sending a structured error response.
 * Includes more details for debugging.
 * @param {object} response - The Express response object.
 * @param {Error} error - The error object.
 * @param {number} statusCode - HTTP status code to send (default: 500).
 */
const handleError = (request, response, error) => {
    const statusCode = error?.statusCode || error?.status || 500;
    const requestId = request?.requestId;

    log("Handling error", {
        request_id: requestId,
        status_code: statusCode,
        error,
    }, "error");

    const userMessage = statusCode >= 500
        ? "An internal error occurred while processing the request"
        : (error?.message || "Request failed");

    const details = error?.response || error?.details || error?.stack || error?.message || "Unknown error";
    response.status(statusCode).json({
        message: userMessage,
        request_id: requestId,
        error: {
            status: statusCode,
            details,
        },
    });
};

// XLSM Parsing and Formatting

/**
 * Normalizes row keys to replace spaces or hyphens with underscores.
 * @param {object} row - Input row object.
 * @returns {object} - Row with normalized keys.
 */
const normalizeRowKeys = (row) => {
    const normalizedData = {};
    Object.keys(row).forEach((key) => {
        const newKey = key.replace(/[- ]/g, '_').trim();
        normalizedData[newKey] = row[key];
    });
    return normalizedData;
};

/**
 * Parses a .xlsm file from a buffer and normalizes the keys.
 * Returns a promise that resolves to an array of parsed objects.
 * @param {Buffer} buffer - The file buffer to parse.
 * @returns {Promise<Array>} - A promise that resolves to an array of parsed data.
 */
const parseXLSMFromBuffer = async (buffer) => {
    try {
        const workbook = new ExcelJS.Workbook();
        await workbook.xlsx.load(buffer);

        if (!Array.isArray(workbook.worksheets) || workbook.worksheets.length === 0) {
            throw new Error("No sheets found in XLSM file");
        }

        const worksheet = workbook.worksheets[0];
        const rawRows = [];
        worksheet.eachRow({ includeEmpty: false }, (row) => {
            const rowValues = [];
            for (let i = 1; i <= row.cellCount; i++) {
                const cell = row.getCell(i);
                rowValues.push(String(cell?.text || "").trim());
            }
            rawRows.push(rowValues);
        });

        if (rawRows.length === 0) {
            throw new Error("No rows found in XLSM file");
        }

        const hasCustPoNumberHeader = (row) =>
            Array.isArray(row) &&
            row.some((cell) =>
                String(cell || "")
                    .trim()
                    .toLowerCase()
                    .replace(/\s+/g, " ")
                    .includes("cust po number")
            );

        const headerRowIndex = rawRows.findIndex(hasCustPoNumberHeader);
        if (headerRowIndex === -1) {
            throw new Error("Could not find header row containing 'Cust PO Number'");
        }

        const headers = rawRows[headerRowIndex].map((cell, index) =>
            String(cell || `column_${index + 1}`).trim()
        );

        const results = rawRows
            .slice(headerRowIndex + 1)
            .filter((row) => Array.isArray(row) && row.some((cell) => String(cell || "").trim() !== ""))
            .map((row) => {
                const mappedRow = {};
                headers.forEach((header, index) => {
                    mappedRow[header] = row[index] || "";
                });
                return normalizeRowKeys(mappedRow);
            });

        log("XLSM parsing completed successfully", {
            sheetName: worksheet.name,
            rowCount: results.length,
        });
        return results;
    } catch (error) {
        log("Error during XLSM parsing", error, "error");
        throw error;
    }
};

/**
 * Formats parsed XLSM data into the required structure for submission.
 * Adds additional error handling per record.
 * @param {Array} results - The parsed XLSM data.
 * @returns {Array} - The formatted data.
 */
const formatCannonHillData = (results) => {
    const shipmentMethodMap = { "G": "Ground", "3RD": "3 Day Select", "2ND": "2nd Day Air" };
    const storeIdMap = { "RTSCS": "68125", "RTFMS": "118741", "HERO": "14077" };
    const uniqueOrderIds = new Set();

    return results.reduce((formattedData, item) => {
        try {
            if (item['Cust_PO_Number']) {
                // Extract the first numeric value from Cust_PO_Number
                const sanitizedValue = item['Cust_PO_Number']
                    .split(/[\/\s-]+/) // Split by "/", spaces, or "-"
                    .find((part) => /^\d+$/.test(part)) // Find the first numeric part
                    ?.trim(); // Trim any extra spaces

                if (sanitizedValue && !uniqueOrderIds.has(sanitizedValue)) {
                    let [carrier_code = "", shipment_method = ""] = (item.Shipped_VIA || "").split('-').map((part) => part.trim());
                    shipment_method = shipmentMethodMap[shipment_method] || shipment_method;

                    const mappedStoreId = storeIdMap[item.Customer_Number];
                    if (!mappedStoreId) {
                        log(`Invalid store ID for Customer_Number: ${item.Customer_Number}`, item, "error");
                    }

                    formattedData.push({
                        source_id: `${mappedStoreId || item.Customer_Number}-${sanitizedValue}`,
                        tracking_number: item.Tracking_Number,
                        carrier_code: carrier_code,
                        shipment_method: shipment_method || "Residential",
                    });
                    uniqueOrderIds.add(sanitizedValue);
                }
            }
        } catch (error) {
            // Log error for this specific record and continue processing
            log("Error processing record in formatCannonHillData", { error, record: item }, "error");
        }
        return formattedData;
    }, []);
};

// API Interaction

/**
 * Sends formatted data to the submit route and handles the response.
 * Contains retry logic with exponential backoff and detailed error logging.
 * @param {Array} data - The formatted data to send.
 * @param {number} retries - Number of retry attempts in case of failure (default: 3).
 * @returns {Promise<object>} - The response from the submit route.
 */
const postToSubmitRoute = async (data, retries = 3) => {
    if (!Array.isArray(data) || data.length === 0) {
        const errorMsg = "No valid data to send to submit route";
        log(errorMsg, data, "error");
        throw new Error(errorMsg);
    }

    const submitRoute = "https://orderdesk-single-order-ship-65ffd8ceba36.herokuapp.com/";
    log("Preparing to send payload to submit route", data);

    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            const response = await fetch(submitRoute, {
                method: 'POST',
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(data),
            });

            const rawResponse = await response.text();
            log("Raw response from submit route", rawResponse);

            if (!response.ok) {
                // Throw error with structured response for retry logic
                throw new Error(JSON.stringify({ status: response.status, response: rawResponse }));
            }

            const jsonResponse = JSON.parse(rawResponse);
            log("Parsed response received from submit route", jsonResponse);

            return {
                status: jsonResponse.status || "success",
                message: jsonResponse.message || "No message provided",
                execution_time: jsonResponse.execution_time || "N/A",
                results: simplifyPostResponses(jsonResponse.results || []),
            };
        } catch (error) {
            log(`Attempt ${attempt} to submit data failed`, error, "error");

            if (attempt === retries) {
                log("All retry attempts failed", error, "error");
                throw error;
            }
            // Exponential backoff delay before the next attempt
            await new Promise((resolve) => setTimeout(resolve, attempt * 1000));
        }
    }
};

/**
 * Simplifies the responses from the submit route.
 * Logs incomplete responses and errors found within.
 * @param {Array} postResponses - The responses from the submit route.
 * @returns {Array} - Simplified responses with status and message.
 */
const simplifyPostResponses = (postResponses) => {
    if (!Array.isArray(postResponses) || postResponses.length === 0) {
        log("No valid responses received from the submit route", postResponses, "error");
        return [{ status: "error", message: "No valid responses received" }];
    }

    return postResponses.map(({ postResponse = {}, error }) => {
        if (error) {
            log("Error in response", error, "error");
            return { status: "error", message: error };
        }

        const status = postResponse.status || "unknown";
        const message = postResponse.message || "No message provided";

        if (!postResponse.status || !postResponse.message) {
            log("Incomplete response detected", postResponse, "error");
        }
        return { status, message };
    });
};

/**
 * Selects a .xlsm file from regular uploads or Mailgun inbound attachments.
 * Supports:
 * - Standard multipart field: "file" (must be .xlsm)
 * - Mailgun inbound fields: "attachment-1", "attachment-2", etc.
 * @param {object} req - Express request.
 * @returns {object|null} - A multer file object or null.
 */
const extractXLSMFileFromRequest = (req) => {
    const files = Array.isArray(req.files)
        ? req.files
        : (req.file ? [req.file] : []);

    if (files.length === 0) {
        return null;
    }

    // Prefer explicit "file" field, then any Mailgun attachment field.
    return (
        files.find((file) =>
            (file.fieldname || "").toLowerCase() === "file" &&
            isXLSMAttachment(file)
        ) ||
        files.find((file) =>
            (file.fieldname || "").toLowerCase().startsWith("attachment-") &&
            isXLSMAttachment(file)
        ) ||
        null
    );
};

/**
 * Handles the `/` POST route for processing uploaded files.
 * Performs file validation, XLSM parsing, data formatting, and submission.
 */
app.post('/', (req, res, next) => {
    upload.any()(req, res, (error) => {
        if (!error) {
            return next();
        }

        if (error instanceof multer.MulterError) {
            if (error.code === "LIMIT_FILE_SIZE") {
                const msg = `Attachment too large. Max allowed size is ${MAX_ATTACHMENT_SIZE_MB}MB`;
                log(msg, { code: error.code }, "error");
                return res.status(413).json({ message: msg, request_id: req.requestId });
            }

            const msg = `Upload rejected: ${error.code}`;
            log(msg, error, "error");
            return res.status(400).json({ message: msg, request_id: req.requestId });
        }

        log("Unexpected upload middleware error", error, "error");
        return res.status(500).json({ message: "Upload middleware failed", request_id: req.requestId });
    });
}, async (req, res, next) => {
    try {
        const contentType = (req.headers["content-type"] || "").toLowerCase();
        const isMultipart = contentType.includes("multipart/form-data");

        if (!isMultipart) {
            const mailgunAttachmentMeta = extractXLSMMailgunAttachmentMeta(req);

            log("Received non-multipart POST / payload (sample/parsed webhook)", {
                request_id: req.requestId,
                headers: {
                    "content-type": req.headers["content-type"],
                    "user-agent": req.headers["user-agent"],
                },
                body: req.body || {},
            });

            if (!mailgunAttachmentMeta) {
                return res.status(202).json({
                    message: "Accepted non-multipart payload. No .xlsm attachment to process.",
                    request_id: req.requestId,
                });
            }

            log("Retrieving stored Mailgun message for .xlsm attachment", {
                request_id: req.requestId,
                originalname: mailgunAttachmentMeta.name,
                content_type: mailgunAttachmentMeta["content-type"],
                size: mailgunAttachmentMeta.size,
                message_url: req.body?.["message-url"],
            });

            const xlsmFile = await fetchMailgunAttachment(req, mailgunAttachmentMeta);
            const results = await parseXLSMFromBuffer(xlsmFile.buffer);
            log("XLSM parsed to JSON, proceeding to data formatting...");

            const formattedData = formatCannonHillData(results);
            log("Data formatted successfully for submission", formattedData);

            const submitResponse = await postToSubmitRoute(formattedData);
            log("Data submitted successfully to the submit route", submitResponse);

            return res.status(200).json({
                ...submitResponse,
                request_id: req.requestId,
            });
        }

        const inboundFiles = Array.isArray(req.files)
            ? req.files.map(({ fieldname, originalname, mimetype, size }) => ({
                fieldname,
                originalname,
                mimetype,
                size,
            }))
            : [];
        log("Incoming payload on POST /", {
            request_id: req.requestId,
            headers: {
                "content-type": req.headers["content-type"],
                "user-agent": req.headers["user-agent"],
            },
            body: req.body || {},
            files: inboundFiles,
        });

        const xlsmFile = extractXLSMFileFromRequest(req);

        // Confirm file attachment in the request
        if (!xlsmFile) {
            const attachmentCount = req.body?.["attachment-count"];
            const msg = attachmentCount
                ? `No .xlsm attachment found in Mailgun payload (attachment-count: ${attachmentCount})`
                : "No .xlsm file attached in the request";
            log(msg, null, "error");
            return res.status(400).json({ message: msg, request_id: req.requestId });
        }

        log("XLSM file received, starting parsing...", {
            fieldname: xlsmFile.fieldname,
            originalname: xlsmFile.originalname,
            mimetype: xlsmFile.mimetype,
            size: xlsmFile.size,
        });
        const results = await parseXLSMFromBuffer(xlsmFile.buffer);
        log("XLSM parsed to JSON, proceeding to data formatting...");

        const formattedData = formatCannonHillData(results);
        log("Data formatted successfully for submission", formattedData);

        const submitResponse = await postToSubmitRoute(formattedData);
        log("Data submitted successfully to the submit route", submitResponse);

        res.status(200).json({
            ...submitResponse,
            request_id: req.requestId,
        });
    } catch (error) {
        next(error);
    }
});

app.use((req, res) => {
    res.status(404).json({
        message: "Route not found",
        request_id: req.requestId,
    });
});

app.use((error, req, res, next) => {
    if (res.headersSent) {
        return next(error);
    }
    handleError(req, res, error);
});

// Server Initialization

const port = process.env.PORT || 3000;
const server = app.listen(port, () => {
    log(`Server running on port ${port}: http://localhost:${port}`);
});

server.on("error", (error) => {
    log("Server failed to start or crashed", error, "error");
    process.exit(1);
});

process.on("unhandledRejection", (reason) => {
    log("Unhandled promise rejection", { reason }, "error");
});

process.on("uncaughtException", (error) => {
    log("Uncaught exception", error, "error");
    setTimeout(() => process.exit(1), 500).unref();
});

process.on("SIGTERM", () => {
    log("SIGTERM received, closing server");
    server.close(() => {
        log("HTTP server closed");
        process.exit(0);
    });
    setTimeout(() => process.exit(1), 10000).unref();
});
