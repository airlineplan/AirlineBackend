# Financial Reporting Acceptance Checklist

1. Run `POST /revenue/backfill-master-fields-to-poo`, then verify POO rows have `depStn`, `arrStn`, `userTag1`, `userTag2`, and `variant` copied from matching Master flights by UTC date, sector, and flight number.
2. In Revenue, compare Total Revenue against Pax Revenue + Cargo Revenue for the same filters and periods.
3. In Dashboard, compare Total revenue against the sum of POO `fnlRccyTotalRev` for the same filters and period.
4. In Dashboard, verify Total fuel cost equals Engine fuel cost + APU fuel cost.
5. In Dashboard, verify Total DOC equals Fuel + Maintenance + Crew + Airport + Navigation + Other DOC.
6. In Dashboard, verify Gross profit/loss equals Total revenue - Total DOC.
7. Change reporting currency and confirm currency pairs regenerate as `LOCAL/REPORTING` at `1.00` across all Master flight dates.
8. Edit one FX rate and confirm subsequent dates for the same pair carry that rate forward until the next edited date.
9. Update POO currency/fare/rate and confirm RCCY revenue uses the carried-forward `POO_CCY/REPORTING_CCY` rate.
10. Recalculate Cost and confirm cost RCCY uses RevenueConfig reporting currency and FX rates.
11. Open Risk Exposure and confirm fuel shows Engine fuel kg + APU fuel kg.
12. Confirm non-reporting currency exposure shows revenue positive and cost negative when source currency values exist.
13. In CostPage, select SN as a filter or grouping and confirm non-maintenance cost categories are disabled.
14. Download Dashboard Excel and confirm filters, financial settings, operational KPIs, and the financial segment are included.
