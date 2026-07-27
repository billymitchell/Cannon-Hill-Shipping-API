import express from 'express';
import bodyParser from 'body-parser';
import { PORT } from './src/config.js';
import { handleError, requestLogger } from './src/http.js';
import { log } from './src/logger.js';
import { shipmentRouter } from './src/routes.js';

const app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(requestLogger);

app.get('/', (req, res) => {
    res.status(200).json({
        status: "ok",
        service: "cannon-hill-shipment-api",
        request_id: req.requestId,
    });
});

app.use(shipmentRouter);

app.use((req, res) => {
    res.status(404).json({
        message: "Route not found",
        request_id: req.requestId,
    });
});

app.use((error, req, res, next) => {
    if (res.headersSent) {
        return next(error);
    }
    return handleError(req, res, error);
});

const server = app.listen(PORT, () => {
    log(`Server running on port ${PORT}: http://localhost:${PORT}`);
});

server.on("error", (error) => {
    log("Server failed to start or crashed", error, "error");
    process.exit(1);
});

process.on("unhandledRejection", (reason) => {
    log("Unhandled promise rejection", { reason }, "error");
});

process.on("uncaughtException", (error) => {
    log("Uncaught exception", error, "error");
    setTimeout(() => process.exit(1), 500).unref();
});

process.on("SIGTERM", () => {
    log("SIGTERM received, closing server");
    server.close(() => {
        log("HTTP server closed");
        process.exit(0);
    });
    setTimeout(() => process.exit(1), 10000).unref();
});
