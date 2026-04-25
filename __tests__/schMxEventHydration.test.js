const assert = require("node:assert/strict");
const { test } = require("node:test");

const { hydrateSchMxEvents } = require("../utils/costLogic");

test("hydrateSchMxEvents fills maintenance logic fields and exact opening balance lookups", () => {
  const hydrated = hydrateSchMxEvents(
    [
      {
        date: "2026-04-02",
        msnEsnApun: "629032",
        event: "E1PR",
        pn: "CFM56-5B",
        snBn: "629032",
        drawdownDate: "2026-04-02",
        mrAccId: "2",
        mrDrawdown: 100,
      },
    ],
    {
      utilisationRows: [
        {
          date: "2026-04-02",
          msnEsn: "629032",
          pn: "CFM56-5B",
          snBn: "629032",
          tsn: 28405,
          csn: 13038,
          dsn: 3913,
        },
      ],
      maintenanceReserveRows: [
        {
          date: "2026-04-02",
          mrAccId: "2",
          msn: "629032",
          closingBal: 3459209,
        },
      ],
    }
  );

  assert.equal(hydrated[0].hours, 28405);
  assert.equal(hydrated[0].cycles, 13038);
  assert.equal(hydrated[0].days, 3913);
  assert.equal(hydrated[0].openingBal, 3459209);
  assert.equal(hydrated[0].remaining, 3459109);
  assert.deepEqual(hydrated[0]._hydratedFields.sort(), ["days", "cycles", "hours", "openingBal", "remaining"].sort());
});

test("hydrateSchMxEvents preserves manual values and leaves opening balance blank without drawdown date", () => {
  const firstPass = hydrateSchMxEvents(
    [
      {
        date: "2026-04-05",
        msnEsnApun: "63190",
        event: "PhCheck",
        pn: "737",
        snBn: "63190",
        hours: "",
        cycles: "",
        days: "",
        drawdownDate: "",
        openingBal: "",
        mrDrawdown: "",
        mrAccId: "1",
      },
    ],
    {
      utilisationRows: [
        {
          date: "2026-04-05",
          msnEsn: "63190",
          pn: "737",
          snBn: "63190",
          tsn: 30540,
          csn: 12582,
          dsn: 3913,
        },
      ],
      maintenanceReserveRows: [],
    }
  );

  assert.equal(firstPass[0].openingBal, "");
  assert.equal(firstPass[0].hours, 30540);
  assert.equal(firstPass[0].cycles, 12582);
  assert.equal(firstPass[0].days, 3913);

  const secondPass = hydrateSchMxEvents(
    [
      {
        ...firstPass[0],
        hours: 99999,
        cycles: 88888,
        days: 77777,
        openingBal: 12345,
      },
    ],
    {
      utilisationRows: [
        {
          date: "2026-04-05",
          msnEsn: "63190",
          pn: "737",
          snBn: "63190",
          tsn: 30540,
          csn: 12582,
          dsn: 3913,
        },
      ],
      maintenanceReserveRows: [],
    }
  );

  assert.equal(secondPass[0].hours, 99999);
  assert.equal(secondPass[0].cycles, 88888);
  assert.equal(secondPass[0].days, 77777);
  assert.equal(secondPass[0].openingBal, 12345);
  assert.deepEqual(
    secondPass[0]._hydratedFields.sort(),
    firstPass[0]._hydratedFields.sort()
  );
});
