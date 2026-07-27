import 'dotenv/config';

const parsedMaxAttachmentSizeMb = Number.parseInt(
    process.env.MAX_ATTACHMENT_SIZE_MB || "10",
    10
);

export const PORT = process.env.PORT || 3000;
export const MAX_ATTACHMENT_SIZE_MB =
    Number.isFinite(parsedMaxAttachmentSizeMb) && parsedMaxAttachmentSizeMb > 0
        ? parsedMaxAttachmentSizeMb
        : 10;
export const MAX_ATTACHMENT_SIZE_BYTES = MAX_ATTACHMENT_SIZE_MB * 1024 * 1024;
export const MAILGUN_API_KEY = process.env.MAILGUN_API_KEY || "";
export const MAILGUN_FETCH_RETRY_DELAYS_MS = [250, 500, 1000, 2000];
export const MAX_ROW_DIAGNOSTICS = 50;
export const SUBMIT_ROUTE =
    process.env.SUBMIT_ROUTE ||
    "https://orderdesk-single-order-ship-65ffd8ceba36.herokuapp.com/";

export const XLSM_MIME_TYPES = new Set([
    "application/vnd.ms-excel.sheet.macroenabled.12",
    "application/octet-stream",
]);
