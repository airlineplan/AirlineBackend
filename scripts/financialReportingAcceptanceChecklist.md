# Financial Reporting Acceptance Checklist

Automated coverage: run `node --test --test-reporter=spec __tests__/dashboardController.test.js`.

Manual UI checks:

1. Open Dashboard monthly with the A100/A101 fixture and confirm period columns show `31 Mar 26` and `30 Apr 26`.
2. Confirm March operational KPIs: departures `1`, seats `180`, pax `153`, pax LF `85%`.
3. Confirm April operational KPIs: departures `1`, seats `180`, pax `160`.
4. Confirm March revenue: pax `144000`, cargo `10`, total `144010`.
5. Confirm April total revenue: `83000`.
6. Confirm March cost: fuel `55000`, total DOC `77000`, gross profit/loss `67010`.
7. Filter by User Tag 1 `Label A`; only March/A100 values remain.
8. Filter by Flight `A101`; only April/A101 values remain.
9. Switch label to `dom`; only domestic A100 flight and revenue remain.
10. Switch label to `intl`; only international A101 flight and revenue remain.
11. Select basis `% of total revenue`; March fuel cost displays `55000 / 144010 * 100`.
12. Select basis `Per ASK`; March total revenue displays `144010 / (180 * 1200)`.
13. Open FX modal with reporting `INR`; confirm generated pairs include `USD/INR` and `EUR/INR`.
14. Change reporting currency to `EUR`; confirm pairs reset to `INR/EUR` and `USD/EUR` at `1.00` for each Master flight date.
15. Edit `USD/INR` on `2026-04-01` to `83`, save, reload, and confirm the carried-forward `2026-04-05` visible rate is `83`.
16. Confirm Dashboard March total revenue matches Revenue page March total with the same filters.
17. Confirm Dashboard costs match Cost page totals with the same filters and period.
18. Open Risk Exposure > Fuel Requirement and confirm total fuel kg equals engine fuel kg plus APU fuel kg.
19. Open Risk Exposure > Currency Exposure and confirm USD revenue is above zero and USD cost is below zero for April.
20. Download Dashboard Excel and confirm applied filters, reporting currency, basis, operational KPI rows, financial segment, period columns, and screen-matching totals.
21. Apply a filter with no matching rows and confirm Dashboard shows zeros/empty chart states without crashing.
