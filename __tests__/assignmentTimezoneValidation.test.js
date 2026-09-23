const assert = require("node:assert/strict");
const { test } = require("node:test");

const { __testables__ } = require("../utils/assignmentSync");

const {
  parseUtcOffsetToMinutes,
  getEffectiveStationOffsetForDate,
  buildFlightInterval,
  intervalsOverlap,
} = __testables__;

const assignmentDate = new Date(Date.UTC(2026, 3, 10));
const station = (stdtz, overrides = {}) => ({
  stationName: overrides.stationName || "TEST",
  stdtz,
  dsttz: overrides.dsttz || stdtz,
  nextDSTStart: overrides.nextDSTStart || "",
  nextDSTEnd: overrides.nextDSTEnd || "",
});

const interval = ({
  date = assignmentDate,
  std,
  sta,
  depOffset,
  arrOffset = depOffset,
  homeOffset = "UTC+0:00",
  departureStation,
  arrivalStation,
}) => buildFlightInterval({
  flightRecord: { std, sta },
  assignDate: date,
  departureStation: departureStation || station(depOffset),
  arrivalStation: arrivalStation || station(arrOffset),
  homeTimezone: homeOffset,
});

test("UTC offset parsing supports zero, positive, negative, half-hour, and quarter-hour values", () => {
  assert.equal(parseUtcOffsetToMinutes("UTC+0:00"), 0);
  assert.equal(parseUtcOffsetToMinutes("UTC+5:30"), 330);
  assert.equal(parseUtcOffsetToMinutes("UTC+5:45"), 345);
  assert.equal(parseUtcOffsetToMinutes("UTC-5:00"), -300);
  assert.equal(parseUtcOffsetToMinutes("UTC+14:30"), null);
  assert.equal(parseUtcOffsetToMinutes("Europe/London"), null);
});

test("departure conversion preserves a preceding Home-TZ calendar day", () => {
  const result = interval({
    std: "01:00",
    sta: "03:00",
    depOffset: "UTC+10:00",
    homeOffset: "UTC+5:30",
  });

  assert.equal(result.stdHomeDateTime.format("YYYY-MM-DD HH:mm"), "2026-04-09 20:30");
  assert.equal(result.stdHomeDateKey, "2026-04-09");
});

test("departure conversion preserves a succeeding Home-TZ calendar day", () => {
  const result = interval({
    std: "20:00",
    sta: "22:00",
    depOffset: "UTC-5:00",
    homeOffset: "UTC+10:00",
  });

  assert.equal(result.stdHomeDateTime.format("YYYY-MM-DD HH:mm"), "2026-04-11 11:00");
  assert.equal(result.stdHomeDateKey, "2026-04-11");
});

test("arrival local date is chosen by absolute instants rather than raw clock ordering", () => {
  const result = interval({
    std: "23:00",
    sta: "20:00",
    depOffset: "UTC+8:00",
    arrOffset: "UTC-5:00",
  });

  assert.equal(result.arrivalLocalDateKey, "2026-04-10");
  assert.equal(result.departureInstantUtc.toISOString(), "2026-04-10T15:00:00.000Z");
  assert.equal(result.arrivalInstantUtc.toISOString(), "2026-04-11T01:00:00.000Z");
});

test("arrival advances its local date when the same-day absolute instant is not after departure", () => {
  const result = interval({
    std: "20:00",
    sta: "22:00",
    depOffset: "UTC-5:00",
    arrOffset: "UTC+10:00",
  });

  assert.equal(result.arrivalLocalDateKey, "2026-04-11");
  assert.equal(result.arrivalInstantUtc.toISOString(), "2026-04-11T12:00:00.000Z");
});

test("DST selection uses standard time outside and daylight time inside inclusive boundaries", () => {
  const dstStation = station("UTC+1:00", {
    dsttz: "UTC+2:00",
    nextDSTStart: "2026-03-29",
    nextDSTEnd: "2026-10-25",
  });

  assert.equal(getEffectiveStationOffsetForDate(dstStation, "2026-03-28"), 60);
  assert.equal(getEffectiveStationOffsetForDate(dstStation, "2026-03-29"), 120);
  assert.equal(getEffectiveStationOffsetForDate(dstStation, "2026-07-01"), 120);
  assert.equal(getEffectiveStationOffsetForDate(dstStation, "2026-10-25"), 120);
  assert.equal(getEffectiveStationOffsetForDate(dstStation, "2026-10-26"), 60);
});

test("arrival offset is re-evaluated after arrival advances across a DST boundary", () => {
  const arrival = station("UTC+1:00", {
    dsttz: "UTC+2:00",
    nextDSTStart: "2026-03-31",
    nextDSTEnd: "2026-10-25",
  });
  const result = interval({
    date: new Date(Date.UTC(2026, 2, 30)),
    std: "23:30",
    sta: "02:30",
    depOffset: "UTC+0:00",
    arrivalStation: arrival,
  });

  assert.equal(result.arrivalLocalDateKey, "2026-03-31");
  assert.equal(result.arrivalEffectiveOffset, 120);
  assert.equal(result.arrivalInstantUtc.toISOString(), "2026-03-31T00:30:00.000Z");
});

test("half-open interval semantics permit back-to-back flights", () => {
  const first = interval({ std: "10:00", sta: "12:00", depOffset: "UTC+0:00" });
  const second = interval({ std: "12:00", sta: "14:00", depOffset: "UTC+0:00" });
  const overlapping = interval({ std: "11:59", sta: "14:00", depOffset: "UTC+0:00" });

  assert.equal(intervalsOverlap(first, second), false);
  assert.equal(intervalsOverlap(first, overlapping), true);
});

test("different source dates can overlap after station-time normalization", () => {
  const first = interval({
    date: new Date(Date.UTC(2026, 3, 10)),
    std: "23:00",
    sta: "02:00",
    depOffset: "UTC+14:00",
  });
  const second = interval({
    date: new Date(Date.UTC(2026, 3, 9)),
    std: "22:00",
    sta: "01:00",
    depOffset: "UTC-12:00",
  });

  assert.equal(first.departureInstantUtc.toISOString(), "2026-04-10T09:00:00.000Z");
  assert.equal(second.departureInstantUtc.toISOString(), "2026-04-10T10:00:00.000Z");
  assert.equal(intervalsOverlap(first, second), true);
});

test("raw local clocks that appear to overlap do not conflict when actual instants differ", () => {
  const first = interval({ std: "10:00", sta: "12:00", depOffset: "UTC+10:00" });
  const second = interval({ std: "11:00", sta: "13:00", depOffset: "UTC-5:00" });

  assert.equal(intervalsOverlap(first, second), false);
});

test("the configured user Home TZ controls the normalized wall datetime", () => {
  const indiaHome = interval({
    std: "10:00",
    sta: "12:00",
    depOffset: "UTC+0:00",
    homeOffset: "UTC+5:30",
  });
  const negativeHome = interval({
    std: "10:00",
    sta: "12:00",
    depOffset: "UTC+0:00",
    homeOffset: "UTC-5:00",
  });

  assert.equal(indiaHome.stdHomeDateTime.format("YYYY-MM-DD HH:mm"), "2026-04-10 15:30");
  assert.equal(negativeHome.stdHomeDateTime.format("YYYY-MM-DD HH:mm"), "2026-04-10 05:00");
});

test("malformed timezone data fails deterministically", () => {
  assert.equal(interval({
    std: "10:00",
    sta: "12:00",
    depOffset: "bad-offset",
  }), null);

  assert.equal(getEffectiveStationOffsetForDate({
    stdtz: "UTC+1:00",
    dsttz: "UTC+2:00",
    nextDSTStart: "2026-03-29",
    nextDSTEnd: "",
  }, "2026-04-10"), null);
});
