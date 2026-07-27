import { MAX_ROW_DIAGNOSTICS } from './config.js';
import { log } from './logger.js';
import { formatCannonHillData, parseXLSMFromBuffer } from './spreadsheet.js';
import { postToSubmitRoute } from './submit.js';

export const processShipmentFile = async (req, xlsmFile) => {
    log("Shipment spreadsheet received", {
        event: "shipment_file_received",
        request_id: req.requestId,
        filename: xlsmFile.originalname,
        content_type: xlsmFile.mimetype,
        size_kb: Math.ceil(
            (xlsmFile.size || xlsmFile.buffer?.length || 0) / 1024
        ),
    });

    const rows = await parseXLSMFromBuffer(xlsmFile.buffer, req.requestId);
    const { shipments, summary, diagnostics } = formatCannonHillData(rows);

    log("Spreadsheet processing completed", {
        event: "spreadsheet_processing_summary",
        request_id: req.requestId,
        ...summary,
    });

    diagnostics.forEach((diagnostic) => {
        log("Spreadsheet row rejected", {
            event: "spreadsheet_row_rejected",
            request_id: req.requestId,
            ...diagnostic,
        }, "warn");
    });

    if (summary.diagnostics_omitted > 0) {
        log("Additional row diagnostics were omitted", {
            event: "spreadsheet_row_diagnostics_truncated",
            request_id: req.requestId,
            diagnostics_omitted: summary.diagnostics_omitted,
            diagnostic_limit: MAX_ROW_DIAGNOSTICS,
        }, "warn");
    }

    if (summary.unknown_customers_skipped > 0) {
        log("Rows with unknown customers were rejected", {
            event: "unknown_customers_rejected",
            request_id: req.requestId,
            rejected_row_count: summary.unknown_customers_skipped,
            customer_numbers: summary.unknown_customers,
        }, "warn");
    }

    if (shipments.length === 0) {
        const error = new Error("Spreadsheet contained no valid shipments");
        error.statusCode = 422;
        error.details = { summary, diagnostics };
        throw error;
    }

    const submitResponse = await postToSubmitRoute(
        shipments,
        3,
        req.requestId
    );

    return {
        ...submitResponse,
        summary,
        diagnostics,
        request_id: req.requestId,
    };
};
