const form = document.getElementById("upload-form");
const fileInput = document.getElementById("file");
const submitButton = document.getElementById("submit-button");
const statusEl = document.getElementById("status");
const reportEl = document.getElementById("report");

const setStatus = (message) => {
  statusEl.textContent = message;
};

const escapeHtml = (value) =>
  String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");

const renderKpi = (label, value) => `
  <div class="kpi">
    <p class="label">${escapeHtml(label)}</p>
    <p class="value">${escapeHtml(value)}</p>
  </div>
`;

const renderDiagnosticsTable = (diagnostics = []) => {
  if (!diagnostics.length) {
    return "<p>No row diagnostics reported.</p>";
  }

  const rows = diagnostics
    .map((item) => `
      <tr>
        <td>${escapeHtml(item.row_number ?? "-")}</td>
        <td>${escapeHtml(item.code ?? "-")}</td>
        <td>${escapeHtml(item.message ?? "-")}</td>
      </tr>
    `)
    .join("");

  return `
    <table>
      <thead>
        <tr>
          <th>Row</th>
          <th>Code</th>
          <th>Message</th>
        </tr>
      </thead>
      <tbody>${rows}</tbody>
    </table>
  `;
};

const renderReport = (payload, isError = false) => {
  const summary = payload?.summary || {};
  const diagnostics = Array.isArray(payload?.diagnostics)
    ? payload.diagnostics
    : [];

  const unknownCustomers = Array.isArray(summary.unknown_customers)
    ? summary.unknown_customers
    : [];

  reportEl.classList.remove("hidden", "success", "error");
  reportEl.classList.add(isError ? "error" : "success");

  reportEl.innerHTML = `
    <h2>${isError ? "Processing Error" : "Processing Complete"}</h2>
    <p><strong>Request ID:</strong> ${escapeHtml(payload?.request_id || "n/a")}</p>
    ${payload?.batch_id ? `<p><strong>Downstream Batch ID:</strong> ${escapeHtml(payload.batch_id)}</p>` : ""}
    ${payload?.status_url ? `<p><strong>Status URL:</strong> ${escapeHtml(payload.status_url)}</p>` : ""}
    ${payload?.replayed ? "<p>This shipment batch was already queued; the existing batch was returned.</p>" : ""}
    <div class="kpis">
      ${renderKpi("Shipments Accepted", summary.shipments_accepted ?? 0)}
      ${renderKpi("Rows Skipped", summary.rows_skipped ?? 0)}
      ${renderKpi("Unknown Customers", summary.unknown_customers_skipped ?? 0)}
      ${renderKpi("Diagnostics Reported", summary.diagnostics_reported ?? diagnostics.length)}
    </div>
    <h3>Skip Breakdown</h3>
    <ul class="list">
      <li>Duplicate Orders: ${escapeHtml(summary.duplicate_orders_skipped ?? 0)}</li>
      <li>Missing PO: ${escapeHtml(summary.missing_po_skipped ?? 0)}</li>
      <li>Invalid PO: ${escapeHtml(summary.invalid_po_skipped ?? 0)}</li>
      <li>Row Errors: ${escapeHtml(summary.row_errors ?? 0)}</li>
    </ul>
    <h3>Unknown Customer Numbers</h3>
    <p>${unknownCustomers.length ? escapeHtml(unknownCustomers.join(", ")) : "None"}</p>
    <h3>Row Diagnostics</h3>
    ${renderDiagnosticsTable(diagnostics)}
  `;
};

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  reportEl.classList.add("hidden");
  reportEl.innerHTML = "";

  const file = fileInput.files?.[0];
  if (!file) {
    setStatus("Please select an .xlsm file.");
    return;
  }

  if (!file.name.toLowerCase().endsWith(".xlsm")) {
    setStatus("Only .xlsm files are supported.");
    return;
  }

  const formData = new FormData();
  formData.append("file", file);

  submitButton.disabled = true;
  setStatus("Processing attachment...");

  try {
    const response = await fetch("/upload", {
      method: "POST",
      body: formData,
    });

    const payload = await response.json().catch(() => ({
      message: "Failed to parse server response",
    }));

    if (!response.ok) {
      const details = payload?.error?.details;
      if (details?.summary) {
        renderReport({
          request_id: payload.request_id,
          summary: details.summary,
          diagnostics: details.diagnostics || [],
        }, true);
      }
      setStatus(payload?.message || "Processing failed.");
      return;
    }

    renderReport(payload, false);
    setStatus("Attachment processed successfully.");
  } catch (error) {
    setStatus("Upload failed. Please try again.");
  } finally {
    submitButton.disabled = false;
  }
});
