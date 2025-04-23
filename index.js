import fs from 'fs';
import csvParser from 'csv-parser';
import fetch from 'node-fetch';
import express from 'express';
import bodyParser from 'body-parser';
import multer from 'multer';
import 'dotenv/config';

import cannonHillRouter from './routes/cannonHill.js';
import submitRouter from './routes/submit.js';

const upload = multer({ dest: 'uploads/' });

let store_key = [
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

const app = express();
app.use(bodyParser.json());

// Mount the route modules
app.use('/cannon-hill', cannonHillRouter);
app.use('/submit', submitRouter);

const port = process.env.PORT || 3000;
app.listen(port, () => {
    console.log(`Server running on port ${port}: http://localhost:${port}`);
});