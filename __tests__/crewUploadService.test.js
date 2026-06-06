const assert = require("node:assert/strict");
const { test } = require("node:test");

const {
  __testables__: {
    normalizeCrewMemberUploadRow,
  },
} = require("../services/crewUploadService");

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
