const SENSITIVE_LOG_KEY_PATTERN =
    /authorization|signature|token|secret|api[-_]?key|body[-_]?(mime|html|plain)|message[-_]?headers|tracking[-_]?number|address|sender|^from$/i;

const sanitizeForLogging = (value, seen = new WeakSet(), key = "") => {
    if (SENSITIVE_LOG_KEY_PATTERN.test(key)) {
        return "[REDACTED]";
    }

    if (value instanceof Error) {
        return {
            name: value.name,
            message: value.message,
            status: value.statusCode || value.status,
        };
    }

    if (typeof value === "string" && value.length > 1000) {
        return `${value.slice(0, 1000)}...<truncated>`;
    }

    if (typeof value === "bigint") {
        return value.toString();
    }

    if (Array.isArray(value)) {
        if (value.length > 25) {
            return { item_count: value.length, contents: "[OMITTED]" };
        }
        return value.map((item) => sanitizeForLogging(item, seen));
    }

    if (value && typeof value === "object") {
        if (seen.has(value)) {
            return "[Circular]";
        }
        seen.add(value);

        const result = {};
        Object.entries(value).forEach(([nestedKey, nestedValue]) => {
            result[nestedKey] = sanitizeForLogging(nestedValue, seen, nestedKey);
        });
        return result;
    }

    return value;
};

export const log = (message, data = null, level = "info") => {
    const levels = { info: console.log, warn: console.warn, error: console.error };
    const logger = levels[level] || console.log;
    const sanitizedData =
        data !== null && data !== undefined ? sanitizeForLogging(data) : null;
    const entry = {
        timestamp: new Date().toISOString(),
        level,
        event: sanitizedData?.event || "application_event",
        message,
    };

    if (sanitizedData && typeof sanitizedData === "object") {
        const { event, ...details } = sanitizedData;
        if (Object.keys(details).length > 0) {
            entry.data = details;
        }
    } else if (sanitizedData !== null) {
        entry.data = sanitizedData;
    }

    logger(JSON.stringify(entry));
};
