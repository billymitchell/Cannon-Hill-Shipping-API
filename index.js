import fs from 'fs';
import csvParser from 'csv-parser';
import express from 'express';
import bodyParser from 'body-parser';
import multer from 'multer';
import stream from 'stream';
import fetch from 'node-fetch';
import 'dotenv/config';

const app = express();
const upload = multer({ storage: multer.memoryStorage() });
app.use(bodyParser.json());

// Utility Functions

/**
 * Logs messages to the console with optional data and log level.
 * Adds more context if available (e.g., error stack).
 * @param {string} message - The message to log.
 * @param {any} data - Additional data to log (optional).
 * @param {string} level - Log level ("info" or "error").
 */
const log = (message, data = null, level = "info") => {
    const levels = { info: console.log, error: console.error };
    if (data && data.stack) {
        // Log stack trace if available
        levels[level](`${message} \nStack: ${data.stack}`);
    } else {
        levels[level](message, data || '');
    }
};

/**
 * Handles errors by logging them and sending a structured error response.
 * Includes more details for debugging.
 * @param {object} response - The Express response object.
 * @param {Error} error - The error object.
 * @param {number} statusCode - HTTP status code to send (default: 500).
 */
const handleError = (response, error, statusCode = 500) => {
    log("Handling error", error, "error");

    let parsedError;
    try {
        parsedError = JSON.parse(error.message || error);
    } catch {
        parsedError = error.message || error;
    }

    response.status(statusCode).json({
        message: "An error occurred while processing the request",
        error: {
            status: statusCode,
            details: parsedError.response || parsedError.stack || parsedError,
        },
    });
};

// CSV Parsing and Formatting

/**
 * Parses a CSV file from a buffer and normalizes the keys.
 * Returns a promise that resolves to an array of parsed objects.
 * @param {Buffer} buffer - The file buffer to parse.
 * @returns {Promise<Array>} - A promise that resolves to an array of parsed data.
 */
const parseCSVFromBuffer = (buffer) => {
    return new Promise((resolve, reject) => {
        const results = [];
        const readableStream = new stream.Readable();
        readableStream.push(buffer);
        readableStream.push(null);

        try {
            readableStream
                .pipe(csvParser({ skipLines: 4 }))
                .on('data', (data) => {
                    // Normalize keys to replace spaces or hyphens with underscores
                    const normalizedData = {};
                    Object.keys(data).forEach((key) => {
                        const newKey = key.replace(/[- ]/g, '_');
                        normalizedData[newKey] = data[key];
                    });
                    results.push(normalizedData);
                })
                .on('end', () => {
                    log("CSV parsing completed successfully", results);
                    resolve(results);
                })
                .on('error', (error) => {
                    log("Error during CSV parsing", error, "error");
                    reject(error);
                });
        } catch (error) {
            // catch synchronous errors in streaming setup (if any)
            log("Synchronous error during CSV parsing setup", error, "error");
            reject(error);
        }
    });
};

/**
 * Formats parsed CSV data into the required structure for submission.
 * Adds additional error handling per record.
 * @param {Array} results - The parsed CSV data.
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
 * Handles the `/` POST route for processing the uploaded CSV files.
 * Performs file validation, CSV parsing, data formatting, and submission.
 */
app.post('/', upload.single('file'), async (req, res) => {
    try {
        // Confirm file attachment in the request
        if (!req.file) {
            const msg = "No file attached in the request";
            log(msg, null, "error");
            return res.status(400).json({ message: msg });
        }

        log("File received, starting CSV parsing...");
        const results = await parseCSVFromBuffer(req.file.buffer);
        log("CSV parsed, proceeding to data formatting...");

        const formattedData = formatCannonHillData(results);
        log("Data formatted successfully for submission", formattedData);

        const submitResponse = await postToSubmitRoute(formattedData);
        log("Data submitted successfully to the submit route", submitResponse);

        res.status(200).json(submitResponse);
    } catch (error) {
        log("Error occurred while processing the request", error, "error");
        handleError(res, error);
    }
});

// Server Initialization

const port = process.env.PORT || 3000;
app.listen(port, () => log(`Server running on port ${port}: http://localhost:${port}`));