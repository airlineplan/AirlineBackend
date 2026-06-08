const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { test } = require("node:test");
const xlsx = require("xlsx");

const {
  importCrewMembers,
  importFlightDuties,
  importOtherDuties,
  readRows,
  __testables__: {
    normalizeCrewMemberUploadRow,
    otherDutyColumnAliases,
    parseOtherDutyTimes,
  },
} = require("../services/crewUploadService");
const { dateKey, diffMinutes, getRowValue } = require("../services/crewTimeUtils");
const {
  CrewFlightAssignment,
  CrewMember,
  CrewOtherDuty,
  CrewUploadBatch,
} = require("../model/crewSchemas");

const writeWorkbook = (rows) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "crew-upload-"));
  const filePath = path.join(dir, "upload.xlsx");
  const workbook = xlsx.utils.book_new();
  xlsx.utils.book_append_sheet(workbook, xlsx.utils.aoa_to_sheet(rows), "Sheet1");
  xlsx.writeFile(workbook, filePath);
  return filePath;
};

const queryResult = (value) => ({ lean: async () => value });

const withPatchedMethods = async (patches, fn) => {
  const originals = patches.map(([target, method]) => [target, method, target[method]]);
  patches.forEach(([target, method, replacement]) => {
    target[method] = replacement;
  });

  try {
    await fn();
  } finally {
    originals.forEach(([target, method, original]) => {
      target[method] = original;
    });
  }
};

const patchUploadBatch = () => [
  CrewUploadBatch,
  "create",
  async () => ({
    _id: "batch-1",
    save: async () => {},
  }),
];

test("crew member upload accepts abbreviated allowance headers", () => {
  const row = {
    ID: "1",
    Name: "Amit",
    "FC/CC": "FC",
    Role: "Captain",
    Base: "BOM",
    "DP allw": "",
    "FDP allw": "1500",
    "FT allw": "",
    "Allowance CCY": "INR",
  };

  assert.deepEqual(normalizeCrewMemberUploadRow(row), {
    crewCode: "1",
    name: "Amit",
    crewType: "FC",
    role: "Captain",
    baseStation: "BOM",
    dpAllowanceRate: 0,
    fdpAllowanceRate: 1500,
    ftAllowanceRate: 0,
    allowanceCurrency: "INR",
  });
});

test("crew member upload ignores header case, spaces, and punctuation", () => {
  const row = {
    " crew id ": "  abc-7 ",
    "CREW   NAME": " Priya ",
    "fc cc": "cc",
    " rank ": "SCC",
    "HOME BASE": " bom ",
    "dp_allw_rate": "1,250",
    "FDP Allow": "",
    "ft.allw": "500",
    " allw ccy ": " inr ",
  };

  assert.deepEqual(normalizeCrewMemberUploadRow(row), {
    crewCode: "ABC-7",
    name: "Priya",
    crewType: "CC",
    role: "SCC",
    baseStation: "BOM",
    dpAllowanceRate: 1250,
    fdpAllowanceRate: 0,
    ftAllowanceRate: 500,
    allowanceCurrency: "INR",
  });
});

test("required Crew Information workbook format parses zero allowance fields", () => {
  const filePath = writeWorkbook([
    ["ID", "Name", "FC/CC", "Role", "Base", "DP allw", "FDP allw", "FT allw", "Allowance CCY", ""],
    ["1", "Amit", "FC", "Captain", "BOM", "0", "1500", "0", "INR", ""],
    ["2", "Vijay", "FC", "FO", "BOM", "0", "0", "800", "INR", ""],
  ]);

  const rows = readRows(filePath);

  assert.equal(rows.length, 2);
  assert.deepEqual(normalizeCrewMemberUploadRow(rows[0]), {
    crewCode: "1",
    name: "Amit",
    crewType: "FC",
    role: "Captain",
    baseStation: "BOM",
    dpAllowanceRate: 0,
    fdpAllowanceRate: 1500,
    ftAllowanceRate: 0,
    allowanceCurrency: "INR",
  });
  assert.deepEqual(normalizeCrewMemberUploadRow(rows[1]), {
    crewCode: "2",
    name: "Vijay",
    crewType: "FC",
    role: "FO",
    baseStation: "BOM",
    dpAllowanceRate: 0,
    fdpAllowanceRate: 0,
    ftAllowanceRate: 800,
    allowanceCurrency: "INR",
  });
});

test("crew upload reader promotes the real header row after a table title", () => {
  const filePath = writeWorkbook([
    ["Flight duty table", "", "", "", "", "", "", "", "", "", "", "", ""],
    ["ID", "Date", "Day", "Flight #", "Dep Stn", "Arr Stn", "Sector", "Captain", "FO", "CC1", "CC2", "CC3", "CC4"],
    ["1", "6 Jun 26", "Sat", "9I611", "BOM", "AMD", "BOM-AMD", "1", "2", "3", "4", "", ""],
  ]);

  const rows = readRows(filePath);

  assert.equal(rows.length, 1);
  assert.equal(rows[0].Date, "6 Jun 26");
  assert.equal(rows[0]["Flight #"], "9I611");
  assert.equal(rows[0]["Dep Stn"], "BOM");
  assert.equal(rows[0].Captain, "1");
});

test("required Flight Duty workbook format parses roster columns after title row", () => {
  const filePath = writeWorkbook([
    ["Flight duty table", "", "", "", "", "", "", "", "", "", "", "", ""],
    ["ID", "Date", "Day", "Flight #", "Dep Stn", "Arr Stn", "Sector", "Captain", "FO", "CC1", "CC2", "CC3", "CC4"],
    ["1", "6 Jun 26", "Sat", "9I611", "BOM", "AMD", "BOM-AMD", "1", "2", "3", "4", "", ""],
  ]);

  const [row] = readRows(filePath);

  assert.equal(getRowValue(row, ["id"]), "1");
  assert.equal(getRowValue(row, ["date"]), "6 Jun 26");
  assert.equal(getRowValue(row, ["flight #", "flight number"]), "9I611");
  assert.equal(getRowValue(row, ["dep stn"]), "BOM");
  assert.equal(getRowValue(row, ["arr stn"]), "AMD");
  assert.equal(getRowValue(row, ["captain"]), "1");
  assert.equal(getRowValue(row, ["fo"]), "2");
  assert.equal(getRowValue(row, ["cc1"]), "3");
  assert.equal(getRowValue(row, ["cc2"]), "4");
});

test("other duty roster uses Crew ID for crew and ID for the roster row", () => {
  const row = {
    ID: "100",
    "Crew ID": "2",
  };

  assert.equal(getRowValue(row, otherDutyColumnAliases.crewCode), "2");
  assert.equal(getRowValue(row, otherDutyColumnAliases.sourceRosterRowId), "100");
});

test("required Other Duty workbook format parses title row and separate date/time columns", () => {
  const filePath = writeWorkbook([
    ["Non-flight / additional duty period", "", "", "", "", "", "", "", "", "", "", ""],
    ["ID", "Date", "Day", "Crew ID", "Location", "Category", "Sub-category", "Start date", "Start time", "Duty time", "Finish date", "Finish time"],
    ["100", "3 Jun 26", "Wed", "2", "DEL", "Training", "CR session", "3 Jun 26", "10:30", "01:00", "3 Jun 26", "11:30"],
  ]);

  const [row] = readRows(filePath);
  const { start, end } = parseOtherDutyTimes(row);

  assert.equal(getRowValue(row, otherDutyColumnAliases.sourceRosterRowId), "100");
  assert.equal(getRowValue(row, otherDutyColumnAliases.crewCode), "2");
  assert.equal(getRowValue(row, ["location"]), "DEL");
  assert.equal(getRowValue(row, ["category"]), "Training");
  assert.equal(getRowValue(row, ["sub-category"]), "CR session");
  assert.equal(dateKey(start), "2026-06-03");
  assert.equal(start.getUTCHours(), 10);
  assert.equal(start.getUTCMinutes(), 30);
  assert.equal(dateKey(end), "2026-06-03");
  assert.equal(end.getUTCHours(), 11);
  assert.equal(end.getUTCMinutes(), 30);
  assert.equal(diffMinutes(start, end), 60);
});

test("blank Crew Information upload replaces prior crew data", async () => {
  const filePath = writeWorkbook([
    ["ID", "Name", "FC/CC", "Role", "Base", "DP allw", "FDP allw", "FT allw", "Allowance CCY"],
    ["", "", "", "", "", "", "", "", ""],
  ]);
  const deleted = [];

  await withPatchedMethods([
    patchUploadBatch(),
    [CrewMember, "deleteMany", async (filter) => { deleted.push(["members", filter]); return { deletedCount: 2 }; }],
    [CrewFlightAssignment, "deleteMany", async (filter) => { deleted.push(["flight", filter]); return { deletedCount: 4 }; }],
    [CrewOtherDuty, "deleteMany", async (filter) => { deleted.push(["other", filter]); return { deletedCount: 3 }; }],
  ], async () => {
    const summary = await importCrewMembers({
      userId: "user-1",
      file: { path: filePath, originalname: "Crew.xlsx" },
      uploadedBy: "tester@example.com",
    });

    assert.equal(summary.rowsRead, 0);
    assert.equal(summary.invalidRows, 0);
    assert.deepEqual(deleted, [
      ["members", { userId: "user-1" }],
      ["flight", { userId: "user-1" }],
      ["other", { userId: "user-1" }],
    ]);
  });
});

test("blank Flight Duty roster upload clears prior flight assignments", async () => {
  const filePath = writeWorkbook([
    ["Flight duty table", "", "", "", "", "", "", "", "", "", "", "", ""],
    ["ID", "Date", "Day", "Flight #", "Dep Stn", "Arr Stn", "Sector", "Captain", "FO", "CC1", "CC2", "CC3", "CC4"],
    ["", "", "", "", "", "", "", "", "", "", "", "", ""],
  ]);
  const deleted = [];

  await withPatchedMethods([
    patchUploadBatch(),
    [CrewMember, "find", () => queryResult([])],
    [CrewFlightAssignment, "deleteMany", async (filter) => { deleted.push(filter); return { deletedCount: 4 }; }],
  ], async () => {
    const summary = await importFlightDuties({
      userId: "user-1",
      file: { path: filePath, originalname: "Flight.xlsx" },
      uploadedBy: "tester@example.com",
    });

    assert.equal(summary.rowsRead, 0);
    assert.equal(summary.invalidRows, 0);
    assert.equal(summary.rowsDeleted, 4);
    assert.deepEqual(deleted, [{ userId: "user-1" }]);
  });
});

test("blank Other Duty roster upload clears prior other duties", async () => {
  const filePath = writeWorkbook([
    ["Non-flight / additional duty period", "", "", "", "", "", "", "", "", "", "", ""],
    ["ID", "Date", "Day", "Crew ID", "Location", "Category", "Sub-category", "Start date", "Start time", "Duty time", "Finish date", "Finish time"],
    ["", "", "", "", "", "", "", "", "", "", "", ""],
  ]);
  const deleted = [];

  await withPatchedMethods([
    patchUploadBatch(),
    [CrewMember, "find", () => queryResult([])],
    [CrewOtherDuty, "deleteMany", async (filter) => { deleted.push(filter); return { deletedCount: 1 }; }],
  ], async () => {
    const summary = await importOtherDuties({
      userId: "user-1",
      file: { path: filePath, originalname: "Other.xlsx" },
      uploadedBy: "tester@example.com",
    });

    assert.equal(summary.rowsRead, 0);
    assert.equal(summary.invalidRows, 0);
    assert.equal(summary.rowsDeleted, 1);
    assert.deepEqual(deleted, [{ userId: "user-1" }]);
  });
});

test("fully invalid Other Duty roster does not clear prior other duties", async () => {
  const filePath = writeWorkbook([
    ["Non-flight / additional duty period", "", "", "", "", "", "", "", "", "", "", ""],
    ["ID", "Date", "Day", "Crew ID", "Location", "Category", "Sub-category", "Start date", "Start time", "Duty time", "Finish date", "Finish time"],
    ["100", "3 Jun 26", "Wed", "", "DEL", "Training", "CR session", "3 Jun 26", "10:30", "01:00", "3 Jun 26", "11:30"],
  ]);
  let deleteCalled = false;

  await withPatchedMethods([
    patchUploadBatch(),
    [CrewMember, "find", () => queryResult([])],
    [CrewOtherDuty, "deleteMany", async () => { deleteCalled = true; return { deletedCount: 1 }; }],
  ], async () => {
    const summary = await importOtherDuties({
      userId: "user-1",
      file: { path: filePath, originalname: "Other.xlsx" },
      uploadedBy: "tester@example.com",
    });

    assert.equal(summary.rowsRead, 1);
    assert.equal(summary.invalidRows, 1);
    assert.equal(deleteCalled, false);
  });
});
