import assert from "node:assert/strict";
import test from "node:test";

import { formatCannonHillData } from "../src/spreadsheet.js";

test("customer 2400 is logged as an E GROUP INC. Email/Callout order", () => {
    const { shipments, summary, diagnostics } = formatCannonHillData([{
        Customer_Number: "2400",
        Cust_PO_Number: "Order 12345",
        Tracking_Number: "not-logged",
        Shipped_VIA: "UPS-G",
    }]);

    assert.deepEqual(shipments, []);
    assert.equal(summary.email_callout_orders_skipped, 1);
    assert.equal(summary.unknown_customers_skipped, 0);
    assert.deepEqual(summary.unknown_customers, []);
    assert.deepEqual(diagnostics, [{
        row_number: 1,
        code: "EMAIL_CALLOUT_ORDER",
        message: "Email/Callout order placed by E GROUP INC.",
        customer_number: "2400",
        customer_name: "E GROUP INC.",
    }]);
});

test("unrecognized customer numbers remain unknown", () => {
    const { summary, diagnostics } = formatCannonHillData([{
        Customer_Number: "9999",
        Cust_PO_Number: "Order 12345",
    }]);

    assert.equal(summary.email_callout_orders_skipped, 0);
    assert.equal(summary.unknown_customers_skipped, 1);
    assert.equal(diagnostics[0].code, "UNKNOWN_CUSTOMER");
});
