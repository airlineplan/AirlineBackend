import os

with open('/home/himanshu/Desktop/Airline/AirlineBackend/controller/userController.js', 'r') as f:
    lines = f.readlines()

def get_lines(ranges):
    # ranges is a list of tuples (start, end) 1-based
    res = []
    for (s, e) in ranges:
        res.extend(lines[s-1:e])
    return "".join(res)

imports = [(1, 40)] # inclusive

helpers = [
    (32, 39), # timeZoneCorrectedDates
    (357, 421), # isValidFlightNumber etc
    (445, 471), # deleteConnections
    (1233, 1242), # regexForFindingSuperset
    (1320, 1423), # calculateTimeDifference to addDays
    (1648, 1693), # normalizeDate, binarySearchByStd
    (2479, 2697), # roundToLastDateOfNextQuarter to timeToMinutes
    (3666, 3670), # parseUTCOffsetToMinutes
    (2927, 2944), # createNewFlights
    (2946, 3025), # eraseAndRepopulateMasterTable
    (2882, 2925), # addRotationDetails
    (3288, 3394), # deleteRotation
]

controllers = {
    "dataController.js": [
        (41, 127),
        (129, 220),
        (435, 443),
        (473, 544),
        (546, 711),
        (713, 810),
        (812, 830),
        (2699, 2811),
        (3604, 3664),
        (3672, 3867)
    ],
    "sectorController.js": [
        (425, 433),
        (834, 884),
        (886, 983),
        (986, 1098),
        (1116, 1129)
    ],
    "flightController.js": [
        (1143, 1164),
        (1166, 1230),
        (1245, 1318),
    ],
    "rotationController.js": [
        (1100, 1114),
        (2045, 2067),
        (2832, 2845),
        (2847, 2880),
        (3028, 3218),
        (3396, 3509),
        (3511, 3602),
    ],
    "stationController.js": [
        (2813, 2830),
        (3221, 3285),
    ],
    "dashboardController.js": [
        (1949, 2017),
        (2069, 2477),
    ],
    "masterController.js": [
        (2020, 2043),
        (3869, 3927),
    ],
    "authController.js": [
        (1131, 1141)
    ]
}

exports_map = {
    "dataController.js": ["AddData", "AddDataFromRotations", "getData", "deleteFlightsAndUpdateSectors", "downloadExpenses", "updateData", "singleData", "getConnections", "getListPageData", "getViewData"],
    "sectorController.js": ["getSecors", "AddSectors", "deleteSectors", "updateSector", "singleSector"],
    "flightController.js": ["getFlights", "searchFlights", "getFlightsWoRotations"],
    "rotationController.js": ["singleRotationDetail", "getRotations", "getNextRotationNumber", "updateRotationSummary", "addRotationDetailsFlgtChange", "deleteCompleteRotation", "deletePrevInRotation"],
    "stationController.js": ["getStationsTableData", "saveStation"],
    "dashboardController.js": ["populateDashboardDropDowns", "getDashboardData"],
    "masterController.js": ["getVariants", "getMasterWeeks"],
    "authController.js": ["AdminLogin"]
}

for ctrl, ranges in controllers.items():
    content = get_lines(imports) + "\n" + get_lines(helpers) + "\n" + get_lines(ranges)
    exp = "module.exports = {\n" + ",\n".join(["  " + e for e in exports_map[ctrl]]) + "\n};\n"
    content += "\n" + exp
    with open('/home/himanshu/Desktop/Airline/AirlineBackend/controller/' + ctrl, 'w') as f:
        f.write(content)
