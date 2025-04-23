# Orderdesk CAWLEY CANNON HILL Shipping API

This project is an API designed to process shipping orders for multiple stores using [Express](https://expressjs.com). It leverages environment variables to securely store API keys for each store.

## Project Structure

- **index.js**:  
  Bootstraps the Express server, loads middlewares, and mounts route handlers from the [routes directory](routes).

- **routes/**:  
  Contains Express route handlers:
  - **cannonHill.js**: Handles routes specific to Cannon Hill.  
  - **submit.js**: Manages order submissions including processing GET/POST requests to Orderdesk's API. See the details in [`submit.js`](routes/submit.js).

- **uploads/**:  
  Directory where uploaded files (such as CSVs) are stored.

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