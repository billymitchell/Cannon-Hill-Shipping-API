# Orderdesk CAWLEY CANNON HILL Shipping API

This project is a shipping-processing service built with [Express](https://expressjs.com). It provides both:

- a browser landing page for manual `.xlsm` uploads and report viewing
- API endpoints for direct uploads and Mailgun webhook-driven processing

It leverages environment variables to securely store runtime configuration and API keys.

## How It Works

1. **Upload Intake**: A landing page at `GET /` lets users upload `.xlsm` files from the browser. API clients can send direct multipart uploads to `POST /upload` (or `POST /` for backward compatibility).
2. **XLSM Parsing**: The uploaded workbook is parsed and converted to JSON rows. The header row is auto-detected by finding the row containing `Cust PO Number`, then keys are normalized (spaces/hyphens replaced with underscores).
3. **Data Formatting**: The parsed JSON data is formatted to match the requirements of the target system. Store/customer IDs and shipment methods are mapped to specific codes. Only unique order IDs are processed.
4. **Submission to Remote API**: The formatted data is sent as a JSON array to the remote API endpoint (`https://orderdesk-single-order-ship-65ffd8ceba36.herokuapp.com/`) via `POST /`. Every request includes a stable content-based `Idempotency-Key`; `X-API-Key` is also included when `SUBMIT_API_KEY` is configured.
5. **Response Handling & User Report**: A `202 Accepted` response means the shipment batch was durably queued, not that every Order Desk shipment has completed. The returned `batch_id`, `status_url`, queue status, processing summary, and row diagnostics are passed back to the caller and rendered by the landing page.
6. **Logging & Error Handling**: Logs contain concise processing events, counts, and safe row-level rejection diagnostics. Known downstream rejections return an actionable message, the spreadsheet summary, row diagnostics, and sanitized downstream validation errors. Unexpected server errors retain the generic message so implementation details are not exposed. Webhook secrets, email bodies, addresses, tracking numbers, and complete shipment payloads are not logged. Row diagnostics are capped at 50 per spreadsheet to keep logs bounded.

**Summary:**
You upload a shipping `.xlsm` file (via UI or API), the server converts it to JSON, formats the data, submits it to a remote order processing API, and returns a structured result (summary + diagnostics). The application is designed for reliability, with detailed logging and error handling throughout.

## Project Structure

- **index.js**:
  Express bootstrap, static-file hosting for the landing page, health endpoint, and process lifecycle handling.

- **public/**:
  Browser UI assets for the landing page upload form and result report rendering.

- **src/config.js**:
  Runtime configuration and environment-variable defaults.

- **src/logger.js** and **src/http.js**:
  Redacted structured logging, request IDs, and HTTP error handling.

- **src/mailgun.js**:
  Mailgun attachment intake, stored-message retrieval, MIME parsing, and upload validation.

- **src/spreadsheet.js**:
  XLSM parsing, shipment formatting, processing summaries, and safe row diagnostics.

- **src/submit.js** and **src/shipment-service.js**:
  Downstream submission and orchestration of the complete shipment workflow.

- **src/routes.js**:
  Direct-upload and Mailgun POST routes (`/`, `/upload`, `/mime`, `/raw-mime`).

- **uploads/**:  
  *(Not used in the current implementation, as files are processed in memory.)*
  Directory where uploaded files (such as XLSM files) would be stored if needed.

- **.env**:  
  Holds runtime configuration such as `PORT`, `MAX_ATTACHMENT_SIZE_MB`, `MAILGUN_API_KEY`, the optional `SUBMIT_ROUTE` override, and `SUBMIT_API_KEY` for an authenticated downstream API.

- **package.json**:  
  Defines project metadata and dependencies including:
  - Express
  - node-fetch
  - dotenv
  - multer
  - exceljs

## Setup & Installation

1. **Install Dependencies:**  
   Run the following command to install dependencies:
   ```sh
   npm install
   ```

2. **Start the Server:**  
   Run:
   ```sh
   node index.js
   ```

3. **Usage:**

### Landing Page (Manual Upload)

- Open `http://localhost:3000/` in a browser.
- Select an `.xlsm` attachment and click **Process Attachment**.
- After processing, the page displays a report including request ID, shipment summary, and row diagnostics.

### API Endpoints

- `GET /`  
  Serves the landing page UI.

- `GET /health`  
  Returns JSON health status.

- `POST /upload`  
  Direct multipart upload endpoint for `.xlsm` files (form-data field: `file`).

- `POST /`  
  Backward-compatible direct upload endpoint and Mailgun-compatible intake path.

- `POST /mime` and `POST /raw-mime`  
  Mailgun-compatible webhook paths.

### Mailgun Webhook Notes

- Inbound Mailgun webhook posts are supported.
- The API accepts multipart attachment fields like `attachment-1`, `attachment-2`, etc.
- Non-`.xlsm` attachments are ignored; only `.xlsm` is processed.

### Downstream Submission Contract

The service submits all accepted spreadsheet rows in one JSON array. Each shipment contains:

```json
{
  "source_id": "68125-123456",
  "tracking_number": "1Z999AA10123456784",
  "carrier_code": "ups",
  "shipment_method": "Ground"
}
```

Configure the protected downstream API with:

```env
SUBMIT_ROUTE=https://your-orderdesk-shipping-api.example.com/
SUBMIT_API_KEY=your-inbound-api-key
```

The downstream API validates the entire array atomically. Invalid store IDs, missing required values, or unsupported carrier codes cause the complete request to be rejected. Retriable responses (`408`, `429`, and `5xx`) use the same idempotency key on every attempt so an uncertain retry cannot create a second batch.

## Notes
- All processing is done in memory; no files are saved to disk.
- The application is designed for reliability, with retry logic and detailed error handling.
- Rows with unknown customer numbers are rejected instead of being submitted with an invalid store ID.
- For more details, see `index.js` and `src/routes.js`.
