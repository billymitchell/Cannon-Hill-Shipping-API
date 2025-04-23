import testResponse from './test-respons.json' assert { type: 'json' };

// Function to extract reasons for failed shipments
function getFailedShipmentReasons(apiResponse) {
    const failedShipments = [];

    if (apiResponse.submitResponse && apiResponse.submitResponse.results) {
        apiResponse.submitResponse.results.forEach(result => {
            if (result.postResponse && result.postResponse.status === "error") {
                failedShipments.push({
                    orderNumber: result.order?.order_number || "Unknown",
                    reason: result.postResponse.message || "No reason provided"
                });
            } else if (result.error) {
                failedShipments.push({
                    orderNumber: result.order?.order_number || "Unknown",
                    reason: result.error
                });
            }
        });
    }

    return failedShipments;
}

// Process the imported JSON data
const failedShipments = getFailedShipmentReasons(testResponse);
console.log(failedShipments);