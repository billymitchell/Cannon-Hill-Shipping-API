# Orderdesk CAWLEY CANNON HILL Shipping API

This project is an API designed to process shipping orders for multiple stores using [Express](https://expressjs.com). It leverages environment variables to securely store API keys for each store.

## How It Works

1. **File Upload**: The API exposes a POST endpoint at `/` that accepts `.xlsm` uploads (using Multer in memory).
2. **XLSM Parsing**: The uploaded workbook is parsed and converted to JSON rows. The header row is auto-detected by finding the row containing `Cust PO Number`, then keys are normalized (spaces/hyphens replaced with underscores).
3. **Data Formatting**: The parsed JSON data is formatted to match the requirements of the target system. Store/customer IDs and shipment methods are mapped to specific codes. Only unique order IDs are processed.
4. **Submission to Remote API**: The formatted data is sent to a remote API endpoint (`https://orderdesk-single-order-ship-65ffd8ceba36.herokuapp.com/`) via POST, with retry logic and exponential backoff for reliability.
5. **Response Handling**: The response from the remote API is simplified and returned with a processing summary. Queued asynchronous submissions are reported as `queued`, not as errors.
6. **Logging & Error Handling**: Logs contain concise processing events, counts, and safe row-level rejection diagnostics. Webhook secrets, email bodies, addresses, tracking numbers, and complete shipment payloads are not logged. Row diagnostics are capped at 50 per spreadsheet to keep logs bounded.

**Summary:**
You upload a shipping `.xlsm` file, the server converts it to JSON, formats the data, then submits it to a remote order processing API and returns the results. The application is designed for reliability, with detailed logging and error handling throughout.

## Project Structure

- **index.js**:
  Small Express server bootstrap, health endpoint, and process lifecycle handling.

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
  Direct-upload and Mailgun POST routes.

- **uploads/**:  
  *(Not used in the current implementation, as files are processed in memory.)*
  Directory where uploaded files (such as XLSM files) would be stored if needed.

- **.env**:  
  Holds runtime configuration such as `PORT`, `MAX_ATTACHMENT_SIZE_MB`, `MAILGUN_API_KEY`, and the optional `SUBMIT_ROUTE` override.

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
   - Send a POST request to `http://localhost:3000/` with a form-data field named `file` containing your `.xlsm` file.
   - Mailgun inbound webhook posts are also supported. The API accepts multipart attachment fields like `attachment-1`, `attachment-2`, etc., ignores non-`.xlsm` attachments, and processes only the `.xlsm` attachment.
   - The server will process the file and return a JSON response with the results.

## Notes
- All processing is done in memory; no files are saved to disk.
- The application is designed for reliability, with retry logic and detailed error handling.
- Rows with unknown customer numbers are rejected instead of being submitted with an invalid store ID.
- For more details, see the code in `index.js`.
