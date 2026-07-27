import multer from 'multer';
import fetch from 'node-fetch';
import {
    MAILGUN_API_KEY,
    MAILGUN_FETCH_RETRY_DELAYS_MS,
    MAX_ATTACHMENT_SIZE_BYTES,
    MAX_ATTACHMENT_SIZE_MB,
    XLSM_MIME_TYPES,
} from './config.js';
import { log } from './logger.js';

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export const isXLSMAttachment = (file = {}) => {
    const fieldName = (file.fieldname || "").toLowerCase();
    const fileName = (file.originalname || "").toLowerCase();
    const mimeType = (file.mimetype || "").toLowerCase();
    const isAttachmentField =
        fieldName === "file" || fieldName.startsWith("attachment-");

    return (
        isAttachmentField &&
        (fileName.endsWith(".xlsm") || XLSM_MIME_TYPES.has(mimeType))
    );
};

export const parseMailgunAttachmentsField = (rawAttachments) => {
    if (!rawAttachments) {
        return [];
    }

    if (Array.isArray(rawAttachments)) {
        return rawAttachments;
    }

    if (typeof rawAttachments !== "string") {
        return [];
    }

    try {
        const parsed = JSON.parse(rawAttachments);
        return Array.isArray(parsed) ? parsed : [];
    } catch (error) {
        log("Failed to parse Mailgun attachments metadata", {
            event: "mailgun_attachment_metadata_invalid",
            error,
        }, "warn");
        return [];
    }
};

export const extractXLSMMailgunAttachmentMeta = (req) => {
    const attachments = parseMailgunAttachmentsField(req.body?.attachments);

    return attachments.find((attachment = {}) => {
        const fileName = String(attachment.name || "").toLowerCase();
        const mimeType = String(
            attachment["content-type"] || attachment.contentType || ""
        ).toLowerCase();

        return fileName.endsWith(".xlsm") || XLSM_MIME_TYPES.has(mimeType);
    }) || null;
};

const decodeMimeHeaderValue = (value = "") =>
    String(value)
        .replace(/\r?\n[ \t]+/g, " ")
        .trim()
        .replace(/^"(.*)"$/, "$1");

const parseMimeHeaders = (rawHeaders = "") =>
    rawHeaders.split(/\r?\n/).reduce((headers, line) => {
        if (!line.trim()) {
            return headers;
        }

        const separatorIndex = line.indexOf(":");
        if (separatorIndex === -1) {
            return headers;
        }

        const key = line.slice(0, separatorIndex).trim().toLowerCase();
        headers[key] = decodeMimeHeaderValue(line.slice(separatorIndex + 1));
        return headers;
    }, {});

const splitMimeParts = (body = "", boundary = "") => {
    if (!boundary) {
        return [];
    }

    const normalizedBoundary = boundary.replace(/^"(.*)"$/, "$1");
    return body
        .split(`--${normalizedBoundary}`)
        .slice(1)
        .map((segment) =>
            segment.replace(/^\r?\n/, "").replace(/\r?\n--\s*$/, "").trim()
        )
        .filter((segment) => segment && segment !== "--");
};

const parseMimePart = (rawPart = "") => {
    const separator = rawPart.includes("\r\n\r\n") ? "\r\n\r\n" : "\n\n";
    const separatorIndex = rawPart.indexOf(separator);

    if (separatorIndex === -1) {
        return { headers: {}, body: rawPart };
    }

    return {
        headers: parseMimeHeaders(rawPart.slice(0, separatorIndex)),
        body: rawPart.slice(separatorIndex + separator.length),
    };
};

const extractBoundary = (contentType = "") => {
    const match = /boundary="?([^";]+)"?/i.exec(contentType);
    return match?.[1] || null;
};

const extractMimeFilename = (contentDisposition = "", contentType = "") => {
    const dispositionMatch = /filename\*?="?([^";]+)"?/i.exec(contentDisposition);
    if (dispositionMatch?.[1]) {
        return dispositionMatch[1];
    }

    const typeMatch = /name="?([^";]+)"?/i.exec(contentType);
    return typeMatch?.[1] || "";
};

const decodeMimeBody = (body = "", transferEncoding = "") => {
    const normalizedEncoding = String(transferEncoding).toLowerCase();
    if (normalizedEncoding === "base64") {
        return Buffer.from(body.replace(/\r?\n/g, ""), "base64");
    }
    return Buffer.from(body, "utf8");
};

const extractXLSMBufferFromMime = (rawMime = "", attachmentMeta = {}) => {
    const separator = rawMime.includes("\r\n\r\n") ? "\r\n\r\n" : "\n\n";
    const separatorIndex = rawMime.indexOf(separator);

    if (separatorIndex === -1) {
        const error = new Error("Stored Mailgun message did not include MIME headers");
        error.statusCode = 502;
        throw error;
    }

    const rootHeaders = parseMimeHeaders(rawMime.slice(0, separatorIndex));
    const rootBody = rawMime.slice(separatorIndex + separator.length);
    const boundary = extractBoundary(rootHeaders["content-type"]);

    if (!boundary) {
        const error = new Error("Stored Mailgun message was not multipart");
        error.statusCode = 502;
        throw error;
    }

    const targetFileName = String(attachmentMeta.name || "").toLowerCase();
    for (const rawPart of splitMimeParts(rootBody, boundary)) {
        const { headers, body } = parseMimePart(rawPart);
        const filename = extractMimeFilename(
            headers["content-disposition"],
            headers["content-type"]
        ).toLowerCase();
        const mimetype = String(headers["content-type"] || "")
            .split(";")[0]
            .trim()
            .toLowerCase();

        if (
            filename === targetFileName ||
            filename.endsWith(".xlsm") ||
            XLSM_MIME_TYPES.has(mimetype)
        ) {
            const buffer = decodeMimeBody(
                body,
                headers["content-transfer-encoding"]
            );

            if (buffer.length > MAX_ATTACHMENT_SIZE_BYTES) {
                const error = new Error(
                    `Attachment too large. Max allowed size is ${MAX_ATTACHMENT_SIZE_MB}MB`
                );
                error.statusCode = 413;
                throw error;
            }

            return {
                fieldname: "mailgun-message-url",
                originalname:
                    filename ||
                    attachmentMeta.name ||
                    "mailgun-attachment.xlsm",
                mimetype:
                    mimetype ||
                    attachmentMeta["content-type"] ||
                    "application/octet-stream",
                size: buffer.length,
                buffer,
            };
        }
    }

    const error = new Error("No .xlsm attachment found in stored Mailgun message");
    error.statusCode = 502;
    throw error;
};

const extractMailgunMessagePath = (messageUrl = "") => {
    if (!messageUrl) {
        return "";
    }

    try {
        const url = new URL(messageUrl);
        return url.pathname.match(/\/v3\/domains\/[^/]+\/messages\/.+$/)?.[0] || "";
    } catch (error) {
        return "";
    }
};

const buildMailgunMessageResourceUrls = (req) => {
    const originalMessageUrl = req.body?.["message-url"] || "";
    const domain = req.body?.domain || "";
    const pathFromOriginalUrl = extractMailgunMessagePath(originalMessageUrl);

    if (!pathFromOriginalUrl && !originalMessageUrl) {
        return [];
    }

    const paths = new Set();
    if (pathFromOriginalUrl) {
        paths.add(pathFromOriginalUrl);
        paths.add(`${pathFromOriginalUrl}/mime`);
    }
    if (originalMessageUrl && !pathFromOriginalUrl) {
        paths.add(originalMessageUrl);
        paths.add(`${originalMessageUrl}/mime`);
    }

    const urls = new Set();
    if (originalMessageUrl) {
        urls.add(originalMessageUrl);
        urls.add(`${originalMessageUrl}/mime`);
    }
    if (pathFromOriginalUrl && domain) {
        for (const host of ["api.mailgun.net", "api.eu.mailgun.net"]) {
            for (const path of paths) {
                urls.add(`https://${host}${path}`);
            }
        }
    }

    return Array.from(urls).filter(Boolean);
};

const buildMailgunAttachmentUrls = (req, attachmentMeta = {}) => {
    const attachmentUrls = new Set();
    const originalAttachmentUrl = attachmentMeta.url || "";
    if (originalAttachmentUrl) {
        attachmentUrls.add(originalAttachmentUrl);
    }

    const domain = req.body?.domain || "";
    const messagePath = extractMailgunMessagePath(req.body?.["message-url"] || "");
    const attachmentIndex = String(originalAttachmentUrl)
        .match(/\/attachments\/(\d+)(?:$|[/?#])/i)?.[1];

    if (domain && messagePath && attachmentIndex) {
        const attachmentPath = `${messagePath}/attachments/${attachmentIndex}`;
        for (const host of ["api.mailgun.net", "api.eu.mailgun.net"]) {
            attachmentUrls.add(`https://${host}${attachmentPath}`);
        }
    }

    return Array.from(attachmentUrls).filter(Boolean);
};

const mailgunAuthorizationHeader = () =>
    `Basic ${Buffer.from(`api:${MAILGUN_API_KEY}`).toString("base64")}`;

const fetchMailgunProtectedResource = async (resourceUrl) => {
    const response = await fetch(resourceUrl, {
        headers: {
            Authorization: mailgunAuthorizationHeader(),
            Accept: "message/rfc2822, application/json;q=0.9, */*;q=0.1",
        },
    });
    const responseBody = await response.text();

    if (!response.ok) {
        const error = new Error(
            `Failed to retrieve Mailgun resource (${response.status})`
        );
        error.statusCode = 502;
        throw error;
    }

    return responseBody;
};

const fetchMailgunProtectedResourceWithRetry = async (resourceUrl) => {
    let lastError = null;

    for (
        let attempt = 0;
        attempt <= MAILGUN_FETCH_RETRY_DELAYS_MS.length;
        attempt += 1
    ) {
        try {
            return await fetchMailgunProtectedResource(resourceUrl);
        } catch (error) {
            lastError = error;
            const isRetriable404 = error?.message?.includes("(404)");
            if (
                !isRetriable404 ||
                attempt === MAILGUN_FETCH_RETRY_DELAYS_MS.length
            ) {
                throw error;
            }

            const retryDelayMs = MAILGUN_FETCH_RETRY_DELAYS_MS[attempt];
            log("Mailgun protected resource not ready yet; retrying", {
                event: "mailgun_resource_retry",
                retry_delay_ms: retryDelayMs,
                attempt: attempt + 1,
            }, "warn");
            await sleep(retryDelayMs);
        }
    }

    throw lastError || new Error("Failed to retrieve Mailgun resource");
};

const fetchFirstSuccessfulMailgunResource = async (resourceUrls = []) => {
    let lastError = null;

    for (const resourceUrl of resourceUrls) {
        try {
            return await fetchMailgunProtectedResourceWithRetry(resourceUrl);
        } catch (error) {
            lastError = error;
            log("Mailgun protected resource retrieval attempt failed", {
                event: "mailgun_resource_retrieval_failed",
                error,
            }, "error");
        }
    }

    throw lastError || new Error("Failed to retrieve Mailgun resource");
};

const fetchMailgunAttachmentByUrl = async (attachmentUrl, attachmentMeta = {}) => {
    if (!attachmentUrl) {
        const error = new Error(
            "Mailgun attachment metadata did not include a downloadable URL"
        );
        error.statusCode = 400;
        throw error;
    }

    const response = await fetch(attachmentUrl, {
        headers: { Authorization: mailgunAuthorizationHeader() },
    });
    if (!response.ok) {
        const error = new Error(
            `Failed to retrieve Mailgun attachment (${response.status})`
        );
        error.statusCode = 502;
        throw error;
    }

    const buffer = Buffer.from(await response.arrayBuffer());
    if (buffer.length > MAX_ATTACHMENT_SIZE_BYTES) {
        const error = new Error(
            `Attachment too large. Max allowed size is ${MAX_ATTACHMENT_SIZE_MB}MB`
        );
        error.statusCode = 413;
        throw error;
    }

    return {
        fieldname: "mailgun-attachment-url",
        originalname: attachmentMeta.name || "mailgun-attachment.xlsm",
        mimetype: attachmentMeta["content-type"] || "application/octet-stream",
        size: buffer.length,
        buffer,
    };
};

const fetchMailgunAttachmentByUrlWithRetry = async (
    attachmentUrl,
    attachmentMeta
) => {
    let lastError = null;

    for (
        let attempt = 0;
        attempt <= MAILGUN_FETCH_RETRY_DELAYS_MS.length;
        attempt += 1
    ) {
        try {
            return await fetchMailgunAttachmentByUrl(
                attachmentUrl,
                attachmentMeta
            );
        } catch (error) {
            lastError = error;
            const isRetriable404 = error?.message?.includes("(404)");
            if (
                !isRetriable404 ||
                attempt === MAILGUN_FETCH_RETRY_DELAYS_MS.length
            ) {
                throw error;
            }

            const retryDelayMs = MAILGUN_FETCH_RETRY_DELAYS_MS[attempt];
            log("Mailgun attachment not ready yet; retrying", {
                event: "mailgun_attachment_retry",
                retry_delay_ms: retryDelayMs,
                attempt: attempt + 1,
            }, "warn");
            await sleep(retryDelayMs);
        }
    }

    throw lastError || new Error("Failed to retrieve Mailgun attachment");
};

export const fetchMailgunAttachment = async (req, attachmentMeta) => {
    if (!MAILGUN_API_KEY) {
        const error = new Error(
            "MAILGUN_API_KEY is required to download Mailgun-hosted attachments"
        );
        error.statusCode = 500;
        throw error;
    }

    const rawMimeFromWebhook =
        req.body?.["body-mime"] || req.body?.mime || "";
    if (rawMimeFromWebhook) {
        return extractXLSMBufferFromMime(rawMimeFromWebhook, attachmentMeta);
    }

    for (const attachmentUrl of buildMailgunAttachmentUrls(req, attachmentMeta)) {
        try {
            return await fetchMailgunAttachmentByUrlWithRetry(
                attachmentUrl,
                attachmentMeta
            );
        } catch (error) {
            log("Direct Mailgun attachment download failed; trying next retrieval option", {
                event: "mailgun_attachment_download_failed",
                error,
            }, "warn");
        }
    }

    const messageUrls = buildMailgunMessageResourceUrls(req);
    if (messageUrls.length === 0) {
        const error = new Error(
            "Mailgun webhook did not include message-url for stored message retrieval"
        );
        error.statusCode = 400;
        throw error;
    }

    const rawResponse = await fetchFirstSuccessfulMailgunResource(messageUrls);
    let rawMime = rawResponse;
    try {
        const parsedResponse = JSON.parse(rawResponse);
        rawMime =
            parsedResponse["body-mime"] || parsedResponse.message || "";
    } catch (error) {
        // The endpoint can also return a raw MIME message body.
    }

    if (!rawMime) {
        const error = new Error(
            "Stored Mailgun message response did not contain a MIME payload"
        );
        error.statusCode = 502;
        throw error;
    }

    return extractXLSMBufferFromMime(rawMime, attachmentMeta);
};

export const upload = multer({
    storage: multer.memoryStorage(),
    limits: {
        fileSize: MAX_ATTACHMENT_SIZE_BYTES,
        files: 20,
    },
    fileFilter: (req, file, cb) => {
        if (isXLSMAttachment(file)) {
            return cb(null, true);
        }
        cb(null, false);
    },
});

export const extractXLSMFileFromRequest = (req) => {
    const files = Array.isArray(req.files)
        ? req.files
        : (req.file ? [req.file] : []);

    return (
        files.find(
            (file) =>
                (file.fieldname || "").toLowerCase() === "file" &&
                isXLSMAttachment(file)
        ) ||
        files.find(
            (file) =>
                (file.fieldname || "").toLowerCase().startsWith("attachment-") &&
                isXLSMAttachment(file)
        ) ||
        null
    );
};
