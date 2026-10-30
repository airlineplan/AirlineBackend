# MR invoice sample demo

The supplied PDF is the acceptance reference for this demo. Support for other client layouts can be customised later. The source PDF is preserved as `__tests__/fixtures/maintenance-reserve/sample-invoice.pdf`; its expected values are recorded independently in `sample-invoice.expected.json` beside it.

## Required demo settings

Use aircraft **VT-ZYX, MSN 4902**. Save these eight MR settings for that registration in Cost settings before uploading the invoice. MR account IDs below are suggested demo IDs; existing matching accounts also work. Use a demo Master planning window beginning on or before **1 September 2026**, since MR settings use the Master start date. The MR date range must cover September 2026 so the schedule includes the **1 October 2026** posting rows.

| MR account | Event | SN | Driver | Currency |
| --- | --- | --- | --- | --- |
| SAMPLE-1 | 6Y SI | 4902 | Month | USD |
| SAMPLE-2 | 12Y SI | 4902 | Month | USD |
| SAMPLE-3 | LG set | Actual landing-gear SN, or 4902 for the demo | Departures | USD |
| SAMPLE-4 | Engine PR | 749014 | FH | USD |
| SAMPLE-5 | Engine LLP | 749014 | Departures | USD |
| SAMPLE-6 | Engine PR | 749015 | FH | USD |
| SAMPLE-7 | Engine LLP | 749015 | Departures | USD |
| SAMPLE-8 | APU PR | P-4275 | APU hours | USD |

## Expected upload and confirmation

1. Open **Upload Invoice** beside the MR schedule download and choose the supplied PDF.
2. Confirm **September 2026**, the stated utilisation month. The invoice date and due date are in October; these do not replace the utilisation month.
3. Confirm **VT-ZYX / MSN 4902**.
4. Select each of the eight extracted sub-account suggestions, check its matched MR setting, and confirm its editable rate, utilisation and contribution. Use **Add another event** between lines, then **Review**.
5. The eight confirmed contributions total **USD 187,327.91**. The review displays both the PDF total and the confirmed total in that currency, including any difference after manual changes.
6. Apply. Eight schedule rows dated **1 October 2026** receive the invoice values and locks. Reapplying the same aircraft/month replaces the invoice without duplicating those rows.

The PDF's sub-accounts are mapped to settings by registration, event and serial number. `ESN … PR`, `ESN … LLP` and `APUN … PR` become Engine PR, Engine LLP and APU PR respectively. Uniquely matching event aliases such as Engine Performance Restoration are also supported. Ambiguous MR settings require manual selection.

If September Invoiced flights have not been created from an actuals upload, Apply still locks the schedule and displays the existing missing-flights warning. A totals warning is expected when flight utilisation differs from the invoice. No sample flight history is invented or amounts adjusted to force reconciliation.

## Verification

- `node --test __tests__/invoiceExtractor.test.js`: parses the actual supplied PDF, verifies the utilisation period, registration/MSN, all eight rates/usages/amounts, engine/APU serials and the total; covers whitespace, event aliases and ambiguous accounts.
- `node --test __tests__/maintenancePlanning.integration.test.js`: uses the supplied PDF through the extraction controller, checks confirmation is required, applies all eight lines, checks October posting locks and values, tests the missing-flights warning and verifies repeat Apply creates no duplicates. Runs only against a temporary database.
- Frontend: `node --test src/Components/Homepage/Tables/invoiceConfirmation.test.js`; checks the correct engine-account match, ambiguous-match handling and sample review total.

Extraction uses the normal table structure and headings, not fixed sample amounts or hard-coded account IDs. Client-specific layouts can be added in `utils/invoiceReserveTable.js`; label-based extraction remains available as a fallback. Automatic OCR for scanned files remains outside this demo scope.
