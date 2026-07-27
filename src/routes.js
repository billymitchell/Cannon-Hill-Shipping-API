import { Router } from 'express';
import multer from 'multer';
import { MAX_ATTACHMENT_SIZE_MB } from './config.js';
import { log } from './logger.js';
import {
    extractXLSMFileFromRequest,
    extractXLSMMailgunAttachmentMeta,
    fetchMailgunAttachment,
    parseMailgunAttachmentsField,
    upload,
} from './mailgun.js';
import { processShipmentFile } from './shipment-service.js';

export const shipmentRouter = Router();

shipmentRouter.post(['/', '/mime', '/raw-mime'], (req, res, next) => {
    upload.any()(req, res, (error) => {
        if (!error) {
            return next();
        }

        if (error instanceof multer.MulterError) {
            if (error.code === "LIMIT_FILE_SIZE") {
                const message =
                    `Attachment too large. Max allowed size is ${MAX_ATTACHMENT_SIZE_MB}MB`;
                log(message, {
                    event: "shipment_file_too_large",
                    request_id: req.requestId,
                    code: error.code,
                }, "warn");
                return res.status(413).json({
                    message,
                    request_id: req.requestId,
                });
            }

            const message = `Upload rejected: ${error.code}`;
            log(message, {
                event: "shipment_upload_rejected",
                request_id: req.requestId,
                code: error.code,
            }, "warn");
            return res.status(400).json({
                message,
                request_id: req.requestId,
            });
        }

        log("Unexpected upload middleware error", {
            event: "shipment_upload_failed",
            request_id: req.requestId,
            error,
        }, "error");
        return res.status(500).json({
            message: "Upload middleware failed",
            request_id: req.requestId,
        });
    });
}, async (req, res, next) => {
    try {
        const contentType = (req.headers["content-type"] || "").toLowerCase();
        const isMultipart = contentType.includes("multipart/form-data");

        if (!isMultipart) {
            const mailgunAttachmentMeta =
                extractXLSMMailgunAttachmentMeta(req);

            log("Mailgun notification received", {
                event: "mailgun_notification_received",
                request_id: req.requestId,
                recipient: req.body?.recipient,
                attachment_count: parseMailgunAttachmentsField(
                    req.body?.attachments
                ).length,
            });

            if (!mailgunAttachmentMeta) {
                log("Mailgun notification contained no shipment spreadsheet", {
                    event: "mailgun_notification_ignored",
                    request_id: req.requestId,
                }, "warn");
                return res.status(202).json({
                    message:
                        "Accepted non-multipart payload. No .xlsm attachment to process.",
                    request_id: req.requestId,
                });
            }

            log("Retrieving stored Mailgun message for .xlsm attachment", {
                event: "mailgun_attachment_retrieving",
                request_id: req.requestId,
                filename: mailgunAttachmentMeta.name,
                content_type: mailgunAttachmentMeta["content-type"],
                size: mailgunAttachmentMeta.size,
            });

            const xlsmFile = await fetchMailgunAttachment(
                req,
                mailgunAttachmentMeta
            );
            const processingResponse = await processShipmentFile(req, xlsmFile);
            return res.status(200).json(processingResponse);
        }

        const inboundFiles = Array.isArray(req.files)
            ? req.files.map(({ originalname, mimetype, size }) => ({
                filename: originalname,
                content_type: mimetype,
                size_kb: Math.ceil((size || 0) / 1024),
            }))
            : [];

        log("Multipart shipment request received", {
            event: "multipart_shipment_request_received",
            request_id: req.requestId,
            recipient: req.body?.recipient,
            attachment_count: inboundFiles.length,
            files: inboundFiles,
        });

        const xlsmFile = extractXLSMFileFromRequest(req);
        if (!xlsmFile) {
            const attachmentCount = req.body?.["attachment-count"];
            const message = attachmentCount
                ? `No .xlsm attachment found in Mailgun payload (attachment-count: ${attachmentCount})`
                : "No .xlsm file attached in the request";
            log(message, {
                event: "shipment_file_missing",
                request_id: req.requestId,
            }, "warn");
            return res.status(400).json({
                message,
                request_id: req.requestId,
            });
        }

        const processingResponse = await processShipmentFile(req, xlsmFile);
        return res.status(200).json(processingResponse);
    } catch (error) {
        return next(error);
    }
});
