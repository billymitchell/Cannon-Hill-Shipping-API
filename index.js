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
    console.error("Error:", error.message || error);

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
            const sanitizedValue = item['Cust_PO_Number'].split('/')[0].replace(/\D/g, '').trim();

            if (sanitizedValue && !uniqueOrderIds.has(sanitizedValue)) {
                let [carrier_code = "", shipment_method = ""] = (item.Shipped_VIA || "").split('-').map((part) => part.trim());
                shipment_method = shipmentMethodMap[shipment_method] || shipment_method;

                const mappedStoreId = storeIdMap[item.Customer_Number] || item.Customer_Number;

                formattedData.push({
                    source_id: `${mappedStoreId}-${sanitizedValue}`,
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
 * @param {Array} data - The formatted data to send.
 * @returns {Promise<object>} - The response from the submit route.
 */
const postToSubmitRoute = async (data) => {
    const submitRoute = "https://orderdesk-single-order-ship-65ffd8ceba36.herokuapp.com/";
    log("Payload being sent to submit route:", data);

    const response = await fetch(submitRoute, {
        method: 'POST',
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
    });

    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(JSON.stringify({ status: response.status, response: errorText }));
    }

    const jsonResponse = await response.json();
    log("Raw response from submit route:", jsonResponse);

    return {
        status: jsonResponse.status || "success",
        message: jsonResponse.message || "No message provided",
        execution_time: jsonResponse.execution_time || "N/A",
        results: simplifyPostResponses(jsonResponse.results || []),
    };
};

/**
 * Simplifies the responses from the submit route.
 * @param {Array} postResponses - The responses from the submit route.
 * @returns {Array} - Simplified responses.
 */
const simplifyPostResponses = (postResponses) =>
    postResponses.map(({ postResponse = {} }) => ({
        status: postResponse.status || "unknown",
        message: postResponse.message || "No message provided",
    }));

// Route Handlers

/**
 * Handles the `/cannon-hill` POST route for processing uploaded CSV files.
 */
app.post('/cannon-hill', upload.single('file'), async (req, res) => {
    try {
        if (!req.file) {
            return res.status(400).json({ message: "No file attached." });
        }

        const results = await parseCSVFromBuffer(req.file.buffer);
        const formattedData = formatCannonHillData(results);
        const submitResponse = await postToSubmitRoute(formattedData);

        res.status(200).json(submitResponse);
    } catch (error) {
        handleError(res, error);
    }
});

// Server Initialization

const port = process.env.PORT || 3000;
app.listen(port, () => log(`Server running on port ${port}: http://localhost:${port}`));