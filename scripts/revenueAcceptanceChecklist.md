# Revenue Acceptance Checklist

Use this checklist after seeding representative Master, POO, and RevenueConfig data.

1. Backfill POO from Master and confirm POO rows contain `depStn`, `arrStn`, `variant`, `userTag1`, and `userTag2`.
2. RevenuePage Dep Stn = `DEL` returns only rows with `depStn = DEL`.
3. RevenuePage User Tag 1 = `Label A` returns only rows enriched from matching Master rows.
4. Revenue Label = Domestic OD returns only `odDI = Dom`.
5. Traffic Class = Leg returns only `trafficType = leg`.
6. Traffic Class = Transit returns only `trafficType` in `transit_fl/transit_sl`.
7. Direct leg pax revenue: 48 pax x 3000 = 144000.
8. Direct leg cargo revenue: 0.2 cargoT x 50 = 10.
9. Direct leg final total revenue: 144000 + 10 = 144010.
10. Collapsed DEL-HYD edit updates both behind and beyond rows.
11. USD/INR exact-date FX rate is used when present.
12. USD/INR carry-forward rate is used when no exact date exists.
13. Group by Sector totals equal the sum of matching sector rows.
14. Group by POO > OD > Flight totals reconcile to Grand Total.
15. Multiple selected metrics display and total independently.
16. Monthly periodicity puts all March 2026 rows under 31 Mar 26.
17. Export includes filters, periodicity, groups, metrics, and screen totals.
18. Dashboard revenue for the same period/filter equals RevenuePage Total Revenue.
19. Blank User Tag 2 filter returns rows where `userTag2` is missing, null, or empty.
20. No matching data shows an empty/zero table without crashing.
