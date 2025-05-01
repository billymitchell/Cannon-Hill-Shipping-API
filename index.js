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
 * @param {string} message - The message to log.
 * @param {any} data - Additional data to log (optional).
 * @param {string} level - Log level ("info" or "error").
 */
const log = (message, data = null, level = "info") => {
    const levels = { info: console.log, error: console.error };
    levels[level](message, data || '');
};

/**
 * Handles errors by logging them and sending a structured error response.
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
            response: parsedError.response || parsedError.stack || parsedError,
        },
    });
};

// CSV Parsing and Formatting

/**
 * Parses a CSV file from a buffer and normalizes the keys.
 * @param {Buffer} buffer - The file buffer to parse.
 * @returns {Promise<Array>} - A promise that resolves to an array of parsed data.
 */
const parseCSVFromBuffer = (buffer) => {
    return new Promise((resolve, reject) => {
        const results = [];
        const readableStream = new stream.Readable();
        readableStream.push(buffer);
        readableStream.push(null);

        readableStream
            .pipe(csvParser({ skipLines: 4 }))
            .on('data', (data) => {
                const normalizedData = {};
                Object.keys(data).forEach((key) => {
                    const newKey = key.replace(/[- ]/g, '_');
                    normalizedData[newKey] = data[key];
                });
                results.push(normalizedData);
            })
            .on('end', () => resolve(results))
            .on('error', (error) => reject(error));
    });
};

/**
 * Formats parsed CSV data into the required structure for submission.
 * @param {Array} results - The parsed CSV data.
 * @returns {Array} - The formatted data.
 */
const formatCannonHillData = (results) => {
    const shipmentMethodMap = { "G": "Ground", "3RD": "3 Day Select", "2ND": "2nd Day Air" };
    const storeIdMap = { "RTSCS": "68125", "RTFMS": "118741", "HERO": "14077" };
    const uniqueOrderIds = new Set();

    return results.reduce((formattedData, item) => {
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
        return formattedData;
    }, []);
};

// API Interaction

/**
 * Sends formatted data to the submit route and handles the response.
 * Includes retry logic and detailed error handling.
 * @param {Array} data - The formatted data to send.
 * @param {number} retries - Number of retry attempts in case of failure (default: 3).
 * @returns {Promise<object>} - The response from the submit route.
 */
const postToSubmitRoute = async (data, retries = 3) => {
    // Validate that the data is an array and not empty
    if (!Array.isArray(data) || data.length === 0) {
        log("No valid data to send to submit route", data, "error");
        throw new Error("No valid data to send to submit route");
    }

    // Define the submit route URL
    const submitRoute = "https://orderdesk-single-order-ship-65ffd8ceba36.herokuapp.com/";
    log("Preparing to send payload to submit route", data);
    log("Payload being sent to submit route", JSON.stringify(data, null, 2));

    // Retry logic with exponential backoff
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            // Send the POST request to the submit route
            const response = await fetch(submitRoute, {
                method: 'POST',
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(data),
            });

            // Read the raw response as text
            const rawResponse = await response.text();
            log("Raw response from submit route", rawResponse);

            // Check if the response status is not OK (e.g., 4xx or 5xx)
            if (!response.ok) {
                throw new Error(JSON.stringify({ status: response.status, response: rawResponse }));
            }

            // Parse the JSON response
            const jsonResponse = JSON.parse(rawResponse);
            log("Parsed response received from submit route", JSON.stringify(jsonResponse, null, 2));

            // Return the structured response
            return {
                status: jsonResponse.status || "success", // Default to "success" if status is missing
                message: jsonResponse.message || "No message provided", // Default message if missing
                execution_time: jsonResponse.execution_time || "N/A", // Default execution time if missing
                results: simplifyPostResponses(jsonResponse.results || []), // Simplify the results array
            };
        } catch (error) {
            // Log the error for the current attempt
            log(`Attempt ${attempt} to submit data failed`, error, "error");

            // If all retries are exhausted, throw the error
            if (attempt === retries) {
                log("All retry attempts failed", error, "error");
                throw error;
            }

            // Wait before retrying (exponential backoff)
            await new Promise((resolve) => setTimeout(resolve, attempt * 1000));
        }
    }
};

/**
 * Simplifies the responses from the submit route.
 * This function processes the array of responses and extracts meaningful information.
 * @param {Array} postResponses - The responses from the submit route.
 * @returns {Array} - Simplified responses with status and message.
 */
const simplifyPostResponses = (postResponses) => {
    // Check if the responses array is valid and not empty
    if (!Array.isArray(postResponses) || postResponses.length === 0) {
        log("No valid responses received from the submit route", postResponses, "error");
        return [{ status: "error", message: "No valid responses received" }];
    }

    // Map through each response and extract relevant details
    return postResponses.map(({ postResponse = {}, error }) => {
        // If there is an error, log it and return an error status
        if (error) {
            log("Error in response", error, "error");
            return { status: "error", message: error };
        }

        // Extract status and message from the postResponse object
        const status = postResponse.status || "unknown"; // Default to "unknown" if status is missing
        const message = postResponse.message || "No message provided"; // Default message if missing

        // Log incomplete responses for debugging
        if (!postResponse.status || !postResponse.message) {
            log("Incomplete response detected", postResponse, "error");
        }

        // Return the simplified response object
        return { status, message };
    });
};

/**
 * Handles the `/` POST route for processing uploaded CSV files.
 */
app.post('/', upload.single('file'), async (req, res) => {
    try {
        // Check if a file is attached in the request
        if (!req.file) {
            log("No file attached in the request", null, "error");
            return res.status(400).json({ message: "No file attached." });
        }

        log("File received, starting CSV parsing...");

        // Parse the CSV file from the buffer
        const results = await parseCSVFromBuffer(req.file.buffer);
        log("CSV parsing completed successfully", results);

        // Format the parsed data into the required structure
        const formattedData = formatCannonHillData(results);
        log("Data formatted successfully", formattedData);

        // Send the formatted data to the submit route
        const submitResponse = await postToSubmitRoute(formattedData);
        log("Data submitted successfully to the submit route", submitResponse);

        // Respond with the result from the submit route
        res.status(200).json(submitResponse);
    } catch (error) {
        // Log the error and send a structured error response
        log("Error occurred while processing the request", error, "error");
        handleError(res, error);
    }
});

// Server Initialization

const port = process.env.PORT || 3000;
app.listen(port, () => log(`Server running on port ${port}: http://localhost:${port}`));