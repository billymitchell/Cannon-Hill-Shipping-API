# Orderdesk CAWLEY CANNON HILL Shipping API

This project is an API designed to process shipping orders for multiple stores using [Express](https://expressjs.com). It leverages environment variables to securely store API keys for each store.

## How It Works

1. **File Upload**: The API exposes a POST endpoint at `/` that accepts a CSV file upload (using Multer in memory).
2. **CSV Parsing**: The uploaded CSV file is parsed, skipping the first 4 lines. Each row is normalized (spaces/hyphens in keys are replaced with underscores).
3. **Data Formatting**: The parsed data is formatted to match the requirements of the target system. Store/customer IDs and shipment methods are mapped to specific codes. Only unique order IDs are processed.
4. **Submission to Remote API**: The formatted data is sent to a remote API endpoint (`https://orderdesk-single-order-ship-65ffd8ceba36.herokuapp.com/`) via POST, with retry logic and exponential backoff for reliability.
5. **Response Handling**: The response from the remote API is simplified and returned to the client. Errors are logged and returned in a structured format.
6. **Logging & Error Handling**: All major steps and errors are logged for debugging. Errors are handled gracefully and returned as JSON.

**Summary:**
You upload a shipping CSV file, the server parses and formats the data, then submits it to a remote order processing API, and returns the results. The application is designed for reliability, with detailed logging and error handling throughout.

## Project Structure

- **index.js**:  
  Main Express server, handles file upload, CSV parsing, data formatting, submission, and error handling.

- **routes/**:  
  *(Not used in the current implementation, but referenced for future modularization.)*
  - **cannonHill.js**: Handles routes specific to Cannon Hill.  
  - **submit.js**: Manages order submissions including processing GET/POST requests to Orderdesk's API.

- **uploads/**:  
  *(Not used in the current implementation, as files are processed in memory.)*
  Directory where uploaded files (such as CSVs) would be stored if needed.

- **.env**:  
  Holds environment variables like store API keys (e.g., `STORE_21633`, `STORE_40348`, etc.).

- **package.json**:  
  Defines project metadata and dependencies including:
  - Express
  - node-fetch
  - dotenv
  - multer
  - csv-parser

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
   - Send a POST request to `http://localhost:3000/` with a form-data field named `file` containing your CSV file.
   - The server will process the file and return a JSON response with the results.

## Notes
- All processing is done in memory; no files are saved to disk.
- The application is designed for reliability, with retry logic and detailed error handling.
- For more details, see the code in `index.js`.