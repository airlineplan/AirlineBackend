# Maintenance planning, actual flights and MR invoices

The application code covers T0–T5. Acceptance is not fully signed off: the target database migration and browser acceptance walkthroughs remain outstanding. The supplied MR invoice is the approved reference for the demo; other client formats can be customised later. Deferred workbook requirements are excluded.

| Package | Implementation | Automated evidence |
| --- | --- | --- |
| T0 Categories and utilisation | Planned query/write guard, category backfill, aircraft/date Actual coverage, Invoiced exclusion | `flightCategoryGuard.test.js`, `utilisationFlights.test.js` |
| T1 Engine operating settings | Fleet button, filtered date grid, CTD validation and persistence, MR recalculation | `maintenancePlanning.integration.test.js`, frontend `maintenanceValidation.test.js` |
| T2 Severity Matrix | Per-account editable matrix, validation, CTD/H:C lookup and escalation, recalculation on Save | `maintenancePlanning.unit.test.js`, `maintenancePlanning.integration.test.js` |
| T3 Major rotables | Optional parent/position, recursive ownership, cycle and conflicting-parent validation, spare utilisation, dashboard projection | `maintenancePlanning.unit.test.js`, `maintenancePlanning.integration.test.js`, `maintenanceDashboardProjection.unit.test.js` |
| T4 Actual flights | Private workbook upload, durable large-import jobs, paired Actual/Invoiced rows, atomic aircraft/date replacement, assignment protection, category reports/exports and persisted costs | `maintenancePlanning.integration.test.js`, category/utilisation tests |
| T5 MR invoices | Private PDF extraction, confirmation wizard, locked schedule, generated versus invoice rates, replacement and reconciliation warnings | `maintenancePlanning.unit.test.js`, `maintenancePlanning.integration.test.js` |

## Confirmed rules

- CTD edits overwrite subsequent stored changes for the same engine and metric, like FX rates. Edits are applied in submission order. Editing November to 23 removes the previously saved December value of 24; December then displays 23. Other engines and metrics retain their own timelines.
- Month-end CTD selects the Severity Matrix column. H:C uses flight FH divided by departures, with Actual flights replacing Planned flights per aircraft/date. Zero departures produce rate zero. Matrix rates between H:C rows use the workbook's average of adjacent rates.
- Actual uploads can contain some aircraft, and must contain all flown flights for every aircraft/date included. Replacement and rejected-group handling use that scope. Planned aircraft snapshots are retained; subsequent assignment uploads skip covered groups.
- Uploaded FT supplies FH; missing FT uses the existing taxi-time calculation. Actual flights override ground days for utilisation. Spare parts stop hours/cycles while calendar days continue.
- Component cost amounts, CCY and RCCY are available in FLGTs and Excel. Other Mx expense components expose utilisation and allocated monthly amounts while retaining the existing total-cost accounting buckets, so aliases are not counted twice.
- Dashboard revenue continues to come from Planned POO, labelled in the UI. Actual and Invoiced costs use stored values. Invoiced fields remain zero apart from applied MR contributions.
- Confirmed invoice contribution controls even when it differs from rate multiplied by utilisation. Reconciliation mismatches warn and apply; no automatic adjustment is made. Planned/Actual continue to use generated values behind invoice locks.
- PDF extraction is local text-layer parsing. Textless/scanned documents support manual confirmation; automatic OCR is not included.

## Deployment prerequisites

1. Use MongoDB with transaction support: a replica set or sharded cluster. Actual uploads reject standalone servers before changing flights or assignments. A failed write rolls back the entire replacement transaction, retaining the previous rows and assignments.
2. Run `npm run migrate:flight-category` against each intended tenant database before release. It sets `Planned` only on documents missing `category`, with the explicit `allCategories` migration option. This migration has been tested on temporary data and has **not** been run against a live database here.
3. Create the new model indexes through the application's normal deployment/index process. Category queries use `{ userId, category, date, flight }`; CTD and invoice/account identities have unique indexes.
4. Use a Node version supported by `pdf-parse`: `>=20.16.0 <21` or `>=22.3.0`. New uploads use generated filenames under `private/uploads/`, excluded from Git and outside static public storage.

## API contract

Paths are relative to the existing authenticated user router. Access uses the existing Fleet, FLGTs and Cost feature permissions. All reads and writes are scoped to the authenticated user.

| Method and path | Input | Result |
| --- | --- | --- |
| `GET /fleet/engine-operating-settings` | Optional engineType, engineVariant, esn, titled, page, limit, from, to | Master date range, date window, paginated engines, values, change points and condition suggestions |
| `PUT /fleet/engine-operating-settings` | `{ changes: [{ esn, metric, date, value }] }` | `{ success, updated }`; recalculates relevant MR schedules |
| `GET /cost-config/severity-matrix/:mrAccId` | MR account ID | `{ data }`; defaults to one Normal/0/0 column and H:C 1.00/3.00 rows |
| `PUT /cost-config/severity-matrix/:mrAccId` | `{ columns: [{ condition, thrust, derate }], rows: [{ hc, rates }] }` | `{ data, schedule }` |
| `POST /flights/upload-actuals` | Multipart `file`, XLSX/XLS, at most 10 MB and 20,000 rows | Import summary; over 500 rows returns HTTP 202 `{ jobId, status }` |
| `GET /flights/actuals-imports/:jobId` | Job ID | Own-job status, result or error |
| `POST /maintenance-reserve/invoices/extract` | Multipart `file`, PDF, at most 10 MB | `{ draftId, textPreview, candidates: { months, aircraft, lines, total? } }`; table totals include amount and currency |
| `POST /maintenance-reserve/invoices/apply` | `{ draftId, acftRegn, msn, month, year, lines: [{ mrAccId, schMxEvent, sn, rate, utilisation, contribution }] }` | `{ invoiceId, checks, status, warning, schedule }` |

Existing FLGTs search, View, List, Dashboard, Cost and Excel export accept `category` with `Planned`, `Actual` or `Invoiced`. The default is Planned. The migration-only `allCategories` option is a model query option, not a public category selection.

## Validation and remaining acceptance work

Backend tests require `mongod` on PATH. Run `npm test`. New integration tests launch an isolated temporary replica set and never connect to application databases. On Windows, older pre-existing test harnesses still pass MongoDB's unsupported `--nounixsocket` flag; the local full-suite run used a temporary Windows-only spawn shim for those existing tests.

Frontend calculation checks: `node --test src/Components/Homepage/Tables/maintenanceValidation.test.js`. Production build: `npm run build`. New feature files pass ESLint; the complete repository lint command still reports pre-existing issues. A per-file comparison against HEAD found no additional lint issues in the eight modified existing frontend files.

Latest verification on 30 October 2026, including the supplied invoice: backend **374 tests, 361 passed, 13 failed**. The failure names exactly match the original baseline (**320 tests, 307 passed, 13 failed**), so all **54 added backend tests pass**. All four frontend calculation/confirmation tests and the production build pass. The previous full frontend lint run reported **674 errors and 16 warnings**; new feature files, including invoice confirmation changes, are clean and modified existing files introduce no additional lint diagnostics.

Automated integration checks cover category reports and Excel contents, CTD overwrite and scoping, Severity Matrix recalculation with Actual utilisation, partial-aircraft uploads and re-uploads, rollback during interrupted replacement, ground-day override, invoice replacement/locking/warnings and nested/spare part accrual including Fleet save preservation. The supplied invoice now also runs through PDF parsing, extraction and Apply, checking all eight September lines and October posting locks. Synthetic digital and textless fixtures retain coverage of the parser fallback. See [MR invoice demo](mr-invoice-demo.md) for the approved sample and setup.

Before acceptance:

- Run the category migration on the intended database and verify category counts.
- For a future client rollout, validate that client's invoice formats and add any required parser customisations. The user approved the single supplied sample as sufficient for the demo.
- Perform browser walkthroughs of the CTD grid, matrix editor, rotable forms, upload summaries, category filters and invoice wizard.
- Resolve or separately track the original backend maintenance test failures and repository-wide frontend lint failures; neither full command is currently green.
