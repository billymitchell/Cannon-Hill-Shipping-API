import express from 'express';
import multer from 'multer';
import fs from 'fs';
import csvParser from 'csv-parser';
import fetch from 'node-fetch';

const router = express.Router();
const upload = multer({ dest: 'uploads/' });

// Utility function for logging
const log = (message, data = null) => {
    if (data) {
        console.log(message, data);
    } else {
        console.log(message);
    }
};

// Utility function for error responses
const handleError = (response, error, statusCode = 500) => {
    console.error("Error:", error.message || error);
    response.status(statusCode).json({
        message: "An error occurred while processing the request",
        error: error.message || error,
    });
};

// Helper function to parse CSV
const parseCSV = (filePath) => {
    return new Promise((resolve, reject) => {
        const results = [];
        fs.createReadStream(filePath)
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

// Helper function to format data
const formatCannonHillData = (results) => {
    const shipmentMethodMap = {
        "G": "Ground",
        "3RD": "3 Day Select",
        "2ND": "2nd Day Air",
    };
    const storeIdMap = {
        "RTSCS": "68125",
        "RTFMS": "118741",
        "HERO": "14077",
    };
    const uniqueOrderIds = new Set();
    const formattedData = [];

    results.forEach((item) => {
        if (item['Cust_PO_Number']) {
            const sanitizedValue = item['Cust_PO_Number']
                .split('/')[0]
                .replace(/\D/g, '')
                .trim();

            if (sanitizedValue && !uniqueOrderIds.has(sanitizedValue)) {
                let [carrier_code = "", shipment_method = ""] = item.Shipped_VIA.split('-').map((part) => part.trim());
                shipment_method = shipmentMethodMap[shipment_method] || shipment_method;

                const formattedDate = new Date(item.Date).toUTCString().replace(' GMT', '');
                const mappedStoreId = storeIdMap[item.Customer_Number] || item.Customer_Number;

                formattedData.push({
                    shipment_date: formattedDate,
                    carrier_code: carrier_code,
                    shipment_method: shipment_method,
                    tracking_number: item.Tracking_Number,
                    order_number: `${mappedStoreId}-${sanitizedValue}`,
                });
                uniqueOrderIds.add(sanitizedValue);
            }
        }
    });

    return formattedData;
};

// Helper function to simplify the response
const simplifyPostResponses = (postResponses) => {
    return postResponses.map((response) => ({
        order_number: response.order_number,
        status: response.status,
        message: response.message,
        execution_time: response.execution_time,
    }));
};

// Helper function to post data
const postToSubmitRoute = async (baseUrl, data) => {
    const response = await fetch(`${baseUrl}/submit`, {
        method: 'POST',
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(data),
    });
    const jsonResponse = await response.json();

    // Simplify the response
    const simplifiedResponse = simplifyPostResponses(jsonResponse.results || []);
    return { originalResponse: jsonResponse, simplifiedResponse };
};

// Main route handler
router.post('/', upload.any(), async (request, response) => {
    try {
        if (!request.files || request.files.length === 0) {
            log("No files attached in the request");
            return response.status(400).json({ message: "No file attached." });
        }

        const csvFile = request.files.find(file => file.mimetype === 'text/csv' || file.originalname.endsWith('.csv'));
        if (!csvFile) {
            log("No CSV file found among attachments");
            return response.status(400).json({ message: "No CSV file found. Ensure you attach a CSV file." });
        }

        log("Found CSV file:", csvFile);
        const results = await parseCSV(csvFile.path);
        const formattedCannonHillData = formatCannonHillData(results);

        log("Formatted CannonHill Data:", formattedCannonHillData);

        const baseUrl = `${request.protocol}://${request.get('host')}`;
        const { originalResponse, simplifiedResponse } = await postToSubmitRoute(baseUrl, formattedCannonHillData);

        log("Response from submit route:", originalResponse);

        // Check if submitResponse is empty or contains errors
        const hasErrors = simplifiedResponse.length === 0 || simplifiedResponse.some(res => res.status === 'error');

        // Include csvData only if there are errors
        const responsePayload = {
            message: "Cannon Hill data received, CSV parsed, and successfully forwarded to submit route",
            formattedCannonHillData: formattedCannonHillData,
            submitResponse: simplifiedResponse,
        };

        if (hasErrors) {
            responsePayload.csvData = results; // Include CSV data only if there are errors
        }

        response.status(200).json(responsePayload);
    } catch (error) {
        handleError(response, error);
    }
});

export default router;