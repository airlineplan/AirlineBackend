const AircraftOnwing = require("../model/aircraftOnwing");
const MaintenanceReserve = require("../model/maintenanceReserveSchema");
const {
  normalizeAircraftOnwing,
  normalizeMaintenanceReserveSchedule,
} = require("./costLogic");

const toValidDate = (value) => {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
};

const getLatestFlightDate = (flights = []) => flights.reduce((latest, flight) => {
  const date = toValidDate(flight?.date);
  if (!date) return latest;
  return !latest || date > latest ? date : latest;
}, null);

const getReserveUpperBound = (latestFlightDate) => {
  if (!latestFlightDate) return null;
  return new Date(Date.UTC(
    latestFlightDate.getUTCFullYear(),
    latestFlightDate.getUTCMonth() + 1,
    1,
    23,
    59,
    59,
    999
  ));
};

const buildMaintenanceReserveContext = async (userId, flights = []) => {
  const latestFlightDate = getLatestFlightDate(flights);
  if (!latestFlightDate) {
    return {
      aircraftOnwing: [],
      maintenanceReserveSchedule: [],
    };
  }

  const reserveUpperBound = getReserveUpperBound(latestFlightDate);
  const [aircraftOnwingRows, maintenanceReserveRows] = await Promise.all([
    AircraftOnwing.find({ userId, date: { $lte: latestFlightDate } })
      .select("date msn pos1Esn pos2Esn apun")
      .sort({ date: 1, msn: 1, _id: 1 })
      .lean(),
    MaintenanceReserve.find({ userId, date: { $lte: reserveUpperBound } })
      .select("date msn mrAccId acftReg rate ccy driver")
      .sort({ date: 1, msn: 1, mrAccId: 1, _id: 1 })
      .lean(),
  ]);

  return {
    aircraftOnwing: normalizeAircraftOnwing(aircraftOnwingRows),
    maintenanceReserveSchedule: normalizeMaintenanceReserveSchedule(maintenanceReserveRows),
  };
};

module.exports = {
  buildMaintenanceReserveContext,
};
