const Station = require("../model/stationSchema");

function timeStrToMinutes(timeStr) {
    if (!timeStr) return 0;
    const parts = String(timeStr).split(':');
    if (parts.length === 2) {
        const hours = parseInt(parts[0], 10) || 0;
        const minutes = parseInt(parts[1], 10) || 0;
        return (hours * 60) + minutes;
    }
    return parseFloat(timeStr) * 60 || 0;
}

async function calculateBH_FH(depStn, arrStn, bt, userId) {
    const depStation = await Station.findOne({ stationName: depStn, userId });
    const arrStation = await Station.findOne({ stationName: arrStn, userId });

    const btMins = timeStrToMinutes(bt);
    const taxiOut = timeStrToMinutes(depStation?.avgTaxiOutTime || "00:00");
    const taxiIn = timeStrToMinutes(arrStation?.avgTaxiInTime || "00:00");

    let fhMins = btMins - taxiOut - taxiIn;
    if (fhMins < 0) fhMins = 0;

    // Helper to format minutes back to "HH:MM"
    const formatHHMM = (mins) => {
        const h = Math.floor(mins / 60);
        const m = Math.floor(mins % 60);
        return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}`;
    };

    return {
        bh: btMins / 60,
        fh: fhMins / 60,
        ft: formatHHMM(fhMins) // Outputs "01:15" if you ever need the string version
    };
}

module.exports = { calculateBH_FH, timeStrToMinutes };