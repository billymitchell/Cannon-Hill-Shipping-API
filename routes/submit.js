import express from 'express';
import fetch from 'node-fetch';

const router = express.Router();

// Array mapping store IDs to their API keys from environment variables.
// These mappings are used to authenticate requests to the Orderdesk API.
const store_key = [
    { STORE_ID: "21633", API_KEY: process.env.STORE_21633 },
    { STORE_ID: "40348", API_KEY: process.env.STORE_40348 },
    { STORE_ID: "12803", API_KEY: process.env.STORE_12803 },
    { STORE_ID: "9672", API_KEY: process.env.STORE_9672 },
    { STORE_ID: "47219", API_KEY: process.env.STORE_47219 },
    { STORE_ID: "8366", API_KEY: process.env.STORE_8366 },
    { STORE_ID: "16152", API_KEY: process.env.STORE_16152 },
    { STORE_ID: "8466", API_KEY: process.env.STORE_8466 },
    { STORE_ID: "15521", API_KEY: process.env.STORE_15521 },
    { STORE_ID: "24121", API_KEY: process.env.STORE_24121 },
    { STORE_ID: "14077", API_KEY: process.env.STORE_14077 },
    { STORE_ID: "12339", API_KEY: process.env.STORE_12339 },
    { STORE_ID: "43379", API_KEY: process.env.STORE_43379 },
    { STORE_ID: "9369", API_KEY: process.env.STORE_9369 },
    { STORE_ID: "9805", API_KEY: process.env.STORE_9805 },
    { STORE_ID: "67865", API_KEY: process.env.STORE_67865 },
    { STORE_ID: "48371", API_KEY: process.env.STORE_48371 },
    { STORE_ID: "48551", API_KEY: process.env.STORE_48551 },
    { STORE_ID: "110641", API_KEY: process.env.STORE_110641 },
    { STORE_ID: "41778", API_KEY: process.env.STORE_41778 },
    { STORE_ID: "8267", API_KEY: process.env.STORE_8267 },
    { STORE_ID: "75092", API_KEY: process.env.STORE_75092 },
    { STORE_ID: "8402", API_KEY: process.env.STORE_8402 },
    { STORE_ID: "68125", API_KEY: process.env.STORE_68125 },
    { STORE_ID: "8729", API_KEY: process.env.STORE_8729 },
    { STORE_ID: "47257", API_KEY: process.env.STORE_47257 },
    { STORE_ID: "8636", API_KEY: process.env.STORE_8636 },
];

// POST endpoint to process order submissions.
// Accepts either a single order or an array of orders.
router.post('/', async (request, response) => {
    try {
        // Support both a single order object or an array of orders.
        const orders = Array.isArray(request.body) ? request.body : [request.body];
        const results = [];
        
        for (const orderPayload of orders) {
            // Retrieve necessary fields from the order payload.
            const tracking_number = orderPayload.tracking_number;
            const shipment_date = orderPayload.shipment_date;
            
            // The order_number is expected to be in the format "STOREID-XXXXXX".
            // Split order_number to extract the store ID.
            const [storeId] = orderPayload.order_number.split('-');
            // The full order_number is also used as the sourceId.
            const sourceId = orderPayload.order_number;

            console.log('Store ID:', storeId, 'Source ID:', sourceId);

            // Find the store object corresponding to the extracted storeId.
            const store = store_key.find(store => store.STORE_ID === storeId);
            if (!store) {
                results.push({ order: orderPayload, error: 'Invalid store ID' });
                continue;
            }

            // Extract the API key from the found store object.
            const apiKey = store.API_KEY;
            if (!apiKey) {
                results.push({ order: orderPayload, error: 'API key not found for the store' });
                continue;
            }

            // Make a GET request to fetch the order using the sourceId.
            const getRequest = await fetch(`https://app.orderdesk.me/api/v2/orders?source_id=${sourceId}`, {
                method: "GET",
                headers: {
                    "ORDERDESK-STORE-ID": storeId,
                    "ORDERDESK-API-KEY": apiKey,
                    "Content-Type": "application/json"
                }
            });

            if (getRequest.ok) {
                // Parse the GET response.
                const orderData = await getRequest.json();
                console.log('GET request successful:', orderData);

                // Ensure there is at least one order in the response.
                if (!orderData.orders || !orderData.orders.length) {
                    results.push({ order: orderPayload, error: 'No orders found in GET response' });
                    continue;
                }

                // Use the first order from the response.
                const order = orderData.orders[0];
                if (!order.id) {
                    results.push({ order: orderPayload, error: 'Order does not contain an id' });
                    continue;
                }
                const order_id = order.id;
                
                // Ensure shipping_method exists and can be split.
                if (!order.shipping_method) {
                    results.push({ order: orderPayload, error: 'Order does not contain shipping_method' });
                    continue;
                }
                const [carrier_code, shipment_method] = order.shipping_method.split(" ");

                // Build the body for the POST request to update the shipment.
                const postBody = {
                    tracking_number: tracking_number,
                    carrier_code: carrier_code,
                    shipment_method: shipment_method,
                    shipment_date: shipment_date
                };

                // Make a POST request to add shipment information.
                const postRequest = await fetch(`https://app.orderdesk.me/api/v2/orders/${order_id}/shipments`, {
                    method: "POST",
                    headers: {
                        "ORDERDESK-STORE-ID": storeId,
                        "ORDERDESK-API-KEY": apiKey,
                        "Content-Type": "application/json"
                    },
                    body: JSON.stringify(postBody)
                });

                // Parse the POST response.
                const postResponse = await postRequest.json();
                console.log('POST request successful:', postResponse);

                results.push({
                    order: orderPayload,
                    getResponse: orderData,
                    postResponse: postResponse
                });
            } else {
                // If the GET request fails, capture the error details.
                const errorData = await getRequest.json();
                console.log('GET request failed:', errorData);
                results.push({ order: orderPayload, error: errorData });
            }
        }
        // Return a consolidated response for all processed orders.
        const hasErrors = results.some(result => result.error);
        const statusCode = hasErrors ? 400 : 200;
        const responseMessage = hasErrors ? 'Not OKAY' : 'Orders processed';

        response.status(statusCode).json({
            message: responseMessage,
            results: results
        });
    } catch (error) {
        // Catch any errors during processing.
        console.error('Error processing orders:', error);
        response.status(400).json({ message: 'Error processing orders', error: error.message });
    }
});

export default router;