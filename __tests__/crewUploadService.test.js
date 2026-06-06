const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { test } = require("node:test");
const xlsx = require("xlsx");

const {
  readRows,
  __testables__: {
    normalizeCrewMemberUploadRow,
    otherDutyColumnAliases,
  },
} = require("../services/crewUploadService");
const { getRowValue } = require("../services/crewTimeUtils");

const writeWorkbook = (rows) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "crew-upload-"));
  const filePath = path.join(dir, "upload.xlsx");
  const workbook = xlsx.utils.book_new();
  xlsx.utils.book_append_sheet(workbook, xlsx.utils.aoa_to_sheet(rows), "Sheet1");
  xlsx.writeFile(workbook, filePath);
  return filePath;
};

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

test("other duty roster uses Crew ID for crew and ID for the roster row", () => {
  const row = {
    ID: "100",
    "Crew ID": "2",
  };

  assert.equal(getRowValue(row, otherDutyColumnAliases.crewCode), "2");
  assert.equal(getRowValue(row, otherDutyColumnAliases.sourceRosterRowId), "100");
});
