import ExcelJS from 'exceljs';
import { MAX_ROW_DIAGNOSTICS } from './config.js';
import { log } from './logger.js';

const SHIPMENT_METHOD_MAP = {
    G: "Ground",
    "3RD": "3 Day Select",
    "2ND": "2nd Day Air",
};

const STORE_ID_MAP = {
    RTSCS: "68125",
    RTFMS: "118741",
    HERO: "14077",
};

const normalizeRowKeys = (row) => {
    const normalizedData = {};
    Object.keys(row).forEach((key) => {
        normalizedData[key.replace(/[- ]/g, '_').trim()] = row[key];
    });
    return normalizedData;
};

export const parseXLSMFromBuffer = async (buffer, requestId = null) => {
    try {
        const workbook = new ExcelJS.Workbook();
        await workbook.xlsx.load(buffer);

        if (!Array.isArray(workbook.worksheets) || workbook.worksheets.length === 0) {
            throw new Error("No sheets found in XLSM file");
        }

        const worksheet = workbook.worksheets[0];
        const rawRows = [];
        const rawRowNumbers = [];

        worksheet.eachRow({ includeEmpty: false }, (row) => {
            const rowValues = [];
            for (let index = 1; index <= row.cellCount; index += 1) {
                rowValues.push(String(row.getCell(index)?.text || "").trim());
            }
            rawRows.push(rowValues);
            rawRowNumbers.push(row.number);
        });

        if (rawRows.length === 0) {
            throw new Error("No rows found in XLSM file");
        }

        const headerRowIndex = rawRows.findIndex(
            (row) =>
                Array.isArray(row) &&
                row.some((cell) =>
                    String(cell || "")
                        .trim()
                        .toLowerCase()
                        .replace(/\s+/g, " ")
                        .includes("cust po number")
                )
        );

        if (headerRowIndex === -1) {
            throw new Error("Could not find header row containing 'Cust PO Number'");
        }

        const headers = rawRows[headerRowIndex].map((cell, index) =>
            String(cell || `column_${index + 1}`).trim()
        );

        const results = rawRows
            .slice(headerRowIndex + 1)
            .map((row, index) => ({
                row,
                rowNumber: rawRowNumbers[headerRowIndex + 1 + index],
            }))
            .filter(
                ({ row }) =>
                    Array.isArray(row) &&
                    row.some((cell) => String(cell || "").trim() !== "")
            )
            .map(({ row, rowNumber }) => {
                const mappedRow = {};
                headers.forEach((header, index) => {
                    mappedRow[header] = row[index] || "";
                });

                const normalizedRow = normalizeRowKeys(mappedRow);
                Object.defineProperty(normalizedRow, "__source_row_number", {
                    value: rowNumber,
                    enumerable: false,
                });
                return normalizedRow;
            });

        log("XLSM parsing completed successfully", {
            event: "spreadsheet_parsed",
            request_id: requestId,
            worksheet: worksheet.name,
            rows_found: results.length,
        });
        return results;
    } catch (error) {
        log("XLSM parsing failed", {
            event: "spreadsheet_parse_failed",
            request_id: requestId,
            error,
        }, "error");
        throw error;
    }
};

export const formatCannonHillData = (results) => {
    const uniqueOrderIds = new Set();
    const unknownCustomers = new Set();
    const shipments = [];
    const diagnostics = [];
    let totalDiagnostics = 0;
    const summary = {
        spreadsheet_rows: Array.isArray(results) ? results.length : 0,
        shipments_accepted: 0,
        rows_skipped: 0,
        duplicate_orders_skipped: 0,
        missing_po_skipped: 0,
        invalid_po_skipped: 0,
        unknown_customers_skipped: 0,
        row_errors: 0,
        unknown_customers: [],
        diagnostics_reported: 0,
        diagnostics_omitted: 0,
    };

    const addDiagnostic = (rowNumber, code, message, details = {}) => {
        totalDiagnostics += 1;
        if (diagnostics.length < MAX_ROW_DIAGNOSTICS) {
            diagnostics.push({
                row_number: rowNumber || null,
                code,
                message,
                ...details,
            });
        }
    };

    (Array.isArray(results) ? results : []).forEach((item, index) => {
        const rowNumber = item?.__source_row_number || index + 1;

        try {
            if (!item?.Cust_PO_Number) {
                summary.missing_po_skipped += 1;
                addDiagnostic(
                    rowNumber,
                    "MISSING_PO",
                    "Customer PO number is missing"
                );
                return;
            }

            const orderId = item.Cust_PO_Number
                .split(/[\/\s-]+/)
                .find((part) => /^\d+$/.test(part))
                ?.trim();

            if (!orderId) {
                summary.invalid_po_skipped += 1;
                addDiagnostic(
                    rowNumber,
                    "INVALID_PO",
                    "Customer PO does not contain a numeric order number"
                );
                return;
            }

            if (uniqueOrderIds.has(orderId)) {
                summary.duplicate_orders_skipped += 1;
                addDiagnostic(
                    rowNumber,
                    "DUPLICATE_ORDER",
                    "Order number was already accepted from an earlier row"
                );
                return;
            }

            const mappedStoreId = STORE_ID_MAP[item.Customer_Number];
            if (!mappedStoreId) {
                summary.unknown_customers_skipped += 1;
                unknownCustomers.add(item.Customer_Number || "missing");
                addDiagnostic(
                    rowNumber,
                    "UNKNOWN_CUSTOMER",
                    "Customer number is not mapped to an OrderDesk store",
                    { customer_number: item.Customer_Number || "missing" }
                );
                return;
            }

            let [carrierCode = "", shipmentMethod = ""] =
                (item.Shipped_VIA || "")
                    .split('-')
                    .map((part) => part.trim());
            shipmentMethod = SHIPMENT_METHOD_MAP[shipmentMethod] || shipmentMethod;

            shipments.push({
                source_id: `${mappedStoreId}-${orderId}`,
                tracking_number: item.Tracking_Number,
                carrier_code: carrierCode,
                shipment_method: shipmentMethod || "Residential",
            });
            uniqueOrderIds.add(orderId);
        } catch (error) {
            summary.row_errors += 1;
            addDiagnostic(
                rowNumber,
                "ROW_PROCESSING_ERROR",
                "An unexpected error occurred while processing this row"
            );
        }
    });

    summary.shipments_accepted = shipments.length;
    summary.rows_skipped = summary.spreadsheet_rows - shipments.length;
    summary.unknown_customers = Array.from(unknownCustomers).sort();
    summary.diagnostics_reported = diagnostics.length;
    summary.diagnostics_omitted = Math.max(
        0,
        totalDiagnostics - diagnostics.length
    );

    return { shipments, summary, diagnostics };
};
