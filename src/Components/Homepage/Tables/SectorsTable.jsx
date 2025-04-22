import React, { useState, useEffect } from "react";
import {
  Table,
  TableHead,
  TableRow,
  TableCell,
  TableBody,
  Stack,
  Typography,
  TextField,
  Menu,
  MenuItem,
  Button,
  Dialog,
  Pagination,
  IconButton,
} from "@mui/material";
import CircularProgress from "@mui/material/CircularProgress";
import DeleteIcon from "@mui/icons-material/Delete";
import Checkbox from "@mui/material/Checkbox";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import AddIcon from "@mui/icons-material/Add";
import DriveFileRenameOutlineIcon from "@mui/icons-material/DriveFileRenameOutline";
import axios from "axios";
import { ToastContainer, toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";
import moment from "moment";
import CopySector from "./CopySector";
import UpdateSectore from "./UpdateSectore";
import CloseIcon from "@mui/icons-material/Close";
import AddSector from "./AddSector";

const RowsPerPage = 8;

const label = { inputProps: { "aria-label": "Checkbox demo" } };

const SectorsTable = () => {
  const [anchorEl, setAnchorEl] = useState(null);
  const [openCopyModal, setOpenCopyModal] = useState(false);
  const [openNewModal, setOpenNewModal] = useState(false);
  const [openUpdateModal, setOpenUpdateModal] = useState(false);
  const [sector, setSector] = useState("");
  const [sector1, setSector1] = useState("");
  const [sector2, setSector2] = useState("");
  const [gcd, setGCD] = useState("");
  const [acftType, setACFTType] = useState("");
  const [variant, setVariant] = useState("");
  const [bt, setBlockTime] = useState("");
  const [paxCapacity, setPaxCapacity] = useState("");
  const [CargoCapT, setCargoCapT] = useState("");
  const [paxLF, setPaxLfPercent] = useState("");
  const [cargoLF, setCargoLfPercent] = useState("");
  const [fromDt, setFromDt] = useState("");
  const [toDt, setToDt] = useState("");
  const [sectorsTableData, setSectorsTableData] = useState([]);
  const [checkedRows, setCheckedRows] = useState([]);
  const [deletedData, setDeletedData] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [arrow, setArrow] = useState({ column: null, direction: "Up" });
  const [loading, setLoading] = useState(true);
  const [arrowDirection, setArrowDirection] = useState(true);
  const [add, setAdd] = useState(true);


  const open = Boolean(anchorEl);
  const handleClick = (event) => {
    setAnchorEl(event.currentTarget);
  };
  const handleClose = () => {
    if (add) {
      setAnchorEl(null);
    }
  };


  var filteredIds = sectorsTableData
    .filter(
      (row) =>
        (row.sector1 || "")
          ?.toLowerCase()
          .includes(sector1?.toLowerCase()) &&
        (row.gcd || "")
          ?.toLowerCase()
          .includes(gcd?.toLowerCase()) &&
        (row.acftType || "")
          ?.toLowerCase()
          .includes(acftType.toLowerCase()) &&
        (row.variant || "")
          ?.toLowerCase()
          .includes(variant.toLowerCase()) &&
        (row.bt || "")?.toLowerCase().includes(bt.toLowerCase()) &&
        (row.paxCapacity || "")
          ?.toLowerCase()
          .includes(paxCapacity.toLowerCase()) &&
        (row.CargoCapT || "")
          ?.toLowerCase()
          .includes(CargoCapT.toLowerCase()) &&
        (row.paxLF || "")
          ?.toLowerCase()
          .includes(paxLF.toLowerCase()) &&
        (row.cargoLF || "")
          ?.toLowerCase()
          .includes(cargoLF.toLowerCase()) &&
        (moment(row.fromDt).format("DD-MMM-YY") || "")
          ?.toLowerCase()
          .includes(fromDt.toLowerCase()) &&
        (moment(row.toDt).format("DD-MMM-YY") || "")?.toLowerCase().includes(toDt.toLowerCase()))
    .map((row) => row._id);

  const sortedData = () => {
    if (!arrow.column) return sectorsTableData;

    const sorted = [...sectorsTableData].sort((a, b) => {
      const colA = a[arrow.column];
      const colB = b[arrow.column];

      if (arrow.direction === "Up") {
        return colA.localeCompare(colB);
      } else {
        return colB.localeCompare(colA);
      }
    });

    return sorted;
  };

  const handleCheckboxChange = (event, rowId, filteredIds) => {
    if (event.target.name === "AllSelect") {
      if (event.target.checked) {
        setCheckedRows(filteredIds);
      } else {
        setCheckedRows([]);
      }
    } else {
      if (event.target.checked) {
        setCheckedRows((prevCheckedRows) => [...prevCheckedRows, rowId]);
      } else {
        setCheckedRows((prevCheckedRows) =>
          prevCheckedRows.filter((id) => id !== rowId)
        );
      }
    }
  };



  //.................................Filter_onChange_handlers...............................

  const handleSector = (event) => {
    setSector(event.target.value);
  };

  const handleSector1 = (event) => {
    setSector1(event.target.value);
  };
  const handleSector2 = (event) => {
    setSector2(event.target.value);
  };
  const handleGCD = (event) => {
    setGCD(event.target.value);
  };
  const handleACFT = (event) => {
    setACFTType(event.target.value);
  };
  const handleVariant = (event) => {
    setVariant(event.target.value);
  };
  const handleBlockTime = (event) => {
    setBlockTime(event.target.value);
  };
  const handlePaxCapacity = (event) => {
    setPaxCapacity(event.target.value);
  };
  const handleCargoCapT = (event) => {
    setCargoCapT(event.target.value);
  };
  const handlePaxPercent = (event) => {
    setPaxLfPercent(event.target.value);
  };
  const handleCargoPercent = (event) => {
    setCargoLfPercent(event.target.value);
  };
  const handleFromDt = (event) => {
    setFromDt(event.target.value);
  };
  const handleToDt = (event) => {
    setToDt(event.target.value);
  };

  useEffect(() => {
    const fetchData = async () => {
      try {
        const accessToken = localStorage.getItem("accessToken");
        const response = await axios.get("https://airlineplan.com/sectors", {
          headers: {
            "x-access-token": accessToken,
          },
        });

        setSectorsTableData(response.data);
        setLoading(false);
      } catch (error) {
        console.error("Error fetching data:", error);
      }
    };

    fetchData();
  }, []);

  const handleDeleteData = async () => {
    const isConfirmed = window.confirm(
      "Are you sure you want to delete this data?"
    );

    if (!isConfirmed) {
      return;
    }

    try {
      const response = await axios.delete("https://airlineplan.com/delete-sector", {
        data: { ids: checkedRows },
      });
      if (
        response.data &&
        response.data.message === "Data deleted successfully"
      ) {
        // createConnections();
        toast.success("Delete Successful");
        setTimeout(() => {
          window.location.reload();
        }, 2000);
      } else {
        toast.error("Delete Failed");
      }
    } catch (error) {
      toast.error("An error occurred while deleting");
      console.error(error);
    }
  };



  const handleArrow = (columnName) => {
    setArrow((prevArrow) => ({
      column: columnName,
      direction:
        prevArrow.column === columnName && prevArrow.direction === "Up"
          ? "Down"
          : "Up",
    }));
  };
  const handleArrowDirection = () => {
    setArrowDirection(!arrowDirection);
  };

  const startIndex = (currentPage - 1) * RowsPerPage;
  const endIndex = startIndex + RowsPerPage;

  const handlePageChange = (event, page) => {
    setCurrentPage(page);
  };

  return (
    <Stack gap={4}>
      <Stack
        direction="row"
        alignItems="center"
        justifyContent="space-between"
        mt="15px"
      >
        <Stack direction="row" alignItems="center" gap="16px">
          {/* <Button
            variant="contained"
            sx={{ textTransform: "capitalize", width: "fit-content" }}
            startIcon={<AddIcon />}
            id="addbutton"
            aria-controls={open ? "addOptions" : undefined}
            aria-haspopup="true"
            aria-expanded={open ? "true" : undefined}
            onClick={handleClick}
          >
            Add
          </Button>
          <Menu
            id="addOptions"
            anchorEl={anchorEl}
            open={open}
            onClose={handleClose}
            MenuListProps={{
              "aria-labelledby": "addbutton",
            }}
          >
            <CopySector checkedRows={checkedRows} setAdd={setAdd}/>
            <AddSector setAdd={setAdd}/>
          </Menu> */}

          {/* // ..........................Update_Row_Form................................ */}

          <UpdateSectore checkedRows={checkedRows} />
        </Stack>
        <Stack>
          <Button
            variant="outlined"
            startIcon={<DeleteIcon />}
            onClick={() => handleDeleteData(checkedRows)}
            sx={{
              textTransform: "capitalize",
              borderColor: "#FF5733",
              color: "#FF5733",
              "&:hover": {
                bgcolor: "#FF5733",
                color: "white",
                borderColor: "#FF5733",
              },
            }}
          >
            Delete
          </Button>
        </Stack>
      </Stack>
      <Stack>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell sx={{ paddingX: "4px" }}>
                <Typography sx={{ fontWeight: "bold", fontSize: "14px" }}>
                  Filter:-
                </Typography>
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="Sector"
                  onChange={handleSector1}
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "14px" },
                  }}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="GCD"
                  onChange={handleGCD}
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "14px" },
                  }}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="ACFT Type"
                  onChange={handleACFT}
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "14px" },
                  }}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="Variant"
                  onChange={handleVariant}
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "14px" },
                  }}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="Block Time"
                  onChange={handleBlockTime}
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "14px" },
                  }}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="Pax Capacity"
                  onChange={handlePaxCapacity}
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "14px" },
                  }}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="Cargo Cap T"
                  onChange={handleCargoCapT}
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "14px" },
                  }}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="Pax LF%"
                  onChange={handlePaxPercent}
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "14px" },
                  }}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="Cargo LF%"
                  onChange={handleCargoPercent}
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "14px" },
                  }}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="From Dt"
                  onChange={handleFromDt}
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "14px" },
                  }}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="To Dt"
                  onChange={handleToDt}
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "14px" },
                  }}
                />
              </TableCell>
            </TableRow>
          </TableHead>
          <TableHead>
            <TableRow sx={{ bgcolor: "#F5F5F5" }}>
              <TableCell
                sx={{
                  fontWeight: "bold",
                  fontSize: "12px",
                  textAlign: "center",
                  padding: "0px",
                }}
              >
                {/* <Checkbox {...label} size="small" /> */}
                <Checkbox
                  {...label}
                  size="small"
                  name="AllSelect"
                  checked={checkedRows.length === filteredIds.length}
                  onChange={(event) =>
                    handleCheckboxChange(event, null, filteredIds)
                  }
                />
              </TableCell>
              <TableCell
                sx={{
                  whiteSpace: "nowrap",
                  fontWeight: "bold",
                  fontSize: "12px",
                  textAlign: "center",
                  padding: "0px",
                }}
              >
                Sector
                <IconButton
                  onClick={() => {
                    handleArrow("sector1");
                    handleArrowDirection();
                  }}
                >
                  {arrowDirection ? (
                    <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                  ) : (
                    <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                  )}
                </IconButton>
              </TableCell>
              <TableCell
                sx={{
                  whiteSpace: "nowrap",
                  fontWeight: "bold",
                  padding: "0px",
                  textAlign: "center",
                  fontSize: "12px",
                }}
              >
                GCD
                <IconButton
                  onClick={() => {
                    handleArrow("gcd");
                    handleArrowDirection();
                  }}
                >
                  {arrowDirection ? (
                    <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                  ) : (
                    <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                  )}
                </IconButton>
              </TableCell>
              <TableCell
                sx={{
                  whiteSpace: "nowrap",
                  fontWeight: "bold",
                  fontSize: "12px",
                  textAlign: "center",
                  padding: "0px",
                }}
              >
                ACFT Type
                <IconButton
                  onClick={() => {
                    handleArrow("acftType");
                    handleArrowDirection();
                  }}
                >
                  {arrowDirection ? (
                    <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                  ) : (
                    <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                  )}
                </IconButton>
              </TableCell>
              <TableCell
                sx={{
                  whiteSpace: "nowrap",
                  fontWeight: "bold",
                  fontSize: "12px",
                  textAlign: "center",
                  padding: "0px",
                }}
              >
                Variant
                <IconButton
                  onClick={() => {
                    handleArrow("variant");
                    handleArrowDirection();
                  }}
                >
                  {arrowDirection ? (
                    <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                  ) : (
                    <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                  )}
                </IconButton>
              </TableCell>
              <TableCell
                sx={{
                  whiteSpace: "nowrap",
                  fontWeight: "bold",
                  fontSize: "12px",
                  textAlign: "center",
                  padding: "0px",
                }}
              >
                Block Time
                <IconButton
                  onClick={() => {
                    handleArrow("bt");
                    handleArrowDirection();
                  }}
                >
                  {arrowDirection ? (
                    <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                  ) : (
                    <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                  )}
                </IconButton>
              </TableCell>
              <TableCell
                sx={{
                  whiteSpace: "nowrap",
                  fontWeight: "bold",
                  fontSize: "12px",
                  textAlign: "center",
                  padding: "0px",
                }}
              >
                Pax Capacity
                <IconButton
                  onClick={() => {
                    handleArrow("paxCapacity");
                    handleArrowDirection();
                  }}
                >
                  {arrowDirection ? (
                    <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                  ) : (
                    <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                  )}
                </IconButton>
              </TableCell>
              <TableCell
                sx={{
                  whiteSpace: "nowrap",
                  fontWeight: "bold",
                  fontSize: "12px",
                  textAlign: "center",
                  padding: "0px",
                }}
              >
                Cargo Cap T
                <IconButton
                  onClick={() => {
                    handleArrow("CargoCapT");
                    handleArrowDirection();
                  }}
                >
                  {arrowDirection ? (
                    <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                  ) : (
                    <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                  )}
                </IconButton>
              </TableCell>
              <TableCell
                sx={{
                  whiteSpace: "nowrap",
                  fontWeight: "bold",
                  fontSize: "12px",
                  textAlign: "center",
                  padding: "0px",
                }}
              >
                Pax LF%
                <IconButton
                  onClick={() => {
                    handleArrow("paxLF");
                    handleArrowDirection();
                  }}
                >
                  {arrowDirection ? (
                    <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                  ) : (
                    <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                  )}
                </IconButton>
              </TableCell>
              <TableCell
                sx={{
                  whiteSpace: "nowrap",
                  fontWeight: "bold",
                  fontSize: "12px",
                  textAlign: "center",
                  padding: "0px",
                }}
              >
                Cargo LF%
                <IconButton
                  onClick={() => {
                    handleArrow("cargoLF");
                    handleArrowDirection();
                  }}
                >
                  {arrowDirection ? (
                    <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                  ) : (
                    <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                  )}
                </IconButton>
              </TableCell>
              <TableCell
                sx={{
                  whiteSpace: "nowrap",
                  fontWeight: "bold",
                  fontSize: "12px",
                  textAlign: "center",
                  padding: "0px",
                }}
              >
                From Dt
                <IconButton
                  onClick={() => {
                    handleArrow("fromDt");
                    handleArrowDirection();
                  }}
                >
                  {arrowDirection ? (
                    <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                  ) : (
                    <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                  )}
                </IconButton>
              </TableCell>
              <TableCell
                sx={{
                  whiteSpace: "nowrap",
                  fontWeight: "bold",
                  fontSize: "12px",
                  textAlign: "center",
                  padding: "0px",
                }}
              >
                To Dt
                <IconButton
                  onClick={() => {
                    handleArrow("toDt");
                    handleArrowDirection();
                  }}
                >
                  {arrowDirection ? (
                    <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                  ) : (
                    <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                  )}
                </IconButton>
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {sortedData()
              .filter(
                (row, index) =>
                  (row.sector1 || "")
                    ?.toLowerCase()
                    .includes(sector1?.toLowerCase()) &&
                  (row.gcd || "")
                    ?.toLowerCase()
                    .includes(gcd?.toLowerCase()) &&
                  (row.acftType || "")
                    ?.toLowerCase()
                    .includes(acftType.toLowerCase()) &&
                  (row.variant || "")
                    ?.toLowerCase()
                    .includes(variant.toLowerCase()) &&
                  (row.bt || "")?.toLowerCase().includes(bt.toLowerCase()) &&
                  (row.paxCapacity || "")
                    ?.toLowerCase()
                    .includes(paxCapacity.toLowerCase()) &&
                  (row.CargoCapT || "")
                    ?.toLowerCase()
                    .includes(CargoCapT.toLowerCase()) &&
                  (row.paxLF || "")
                    ?.toLowerCase()
                    .includes(paxLF.toLowerCase()) &&
                  (row.cargoLF || "")
                    ?.toLowerCase()
                    .includes(cargoLF.toLowerCase()) &&
                  (moment(row.fromDt).format("DD-MMM-YY") || "")
                    ?.toLowerCase()
                    .includes(fromDt.toLowerCase()) &&
                  (moment(row.toDt).format("DD-MMM-YY") || "")?.toLowerCase().includes(toDt.toLowerCase())
              )
              .slice(startIndex, endIndex)
              ?.map((row, index) => (
                <TableRow key={index} sx={{ backgroundColor: index % 2 !== 0 ? '#f0f0f0' : 'inherit' }}>
                  <TableCell
                    sx={{
                      fontSize: "12px",
                      padding: "0px",
                      textAlign: "center",
                    }}
                  >
                    {loading ? (
                      <CircularProgress
                        sx={{ display: "flex", marginLeft: "800px" }}
                      />
                    ) : (
                      <Checkbox
                        checked={checkedRows.includes(row._id)}
                        onChange={(event) => handleCheckboxChange(event, row._id)}
                      />
                    )}
                  </TableCell>
                  <TableCell
                    sx={{
                      whiteSpace: "nowrap",
                      fontSize: "12px",
                      padding: "0px",
                      textAlign: "center",
                    }}
                  >
                    {row.sector1}-{row.sector2}
                  </TableCell>
                  <TableCell
                    sx={{
                      whiteSpace: "nowrap",
                      fontSize: "12px",
                      padding: "0px",
                      textAlign: "center",
                    }}
                  >
                    {row.gcd}
                  </TableCell>
                  <TableCell
                    sx={{
                      fontSize: "12px",
                      padding: "0px",
                      textAlign: "center",
                    }}
                  >
                    {row.acftType}
                  </TableCell>
                  <TableCell
                    sx={{
                      fontSize: "12px",
                      padding: "0px",
                      textAlign: "center",
                    }}
                  >
                    {row.variant}
                  </TableCell>
                  <TableCell
                    sx={{
                      fontSize: "12px",
                      padding: "0px",
                      textAlign: "center",
                    }}
                  >
                    {row.bt}
                  </TableCell>
                  <TableCell
                    sx={{
                      fontSize: "12px",
                      padding: "0px",
                      textAlign: "center",
                    }}
                  >
                    {row.paxCapacity}
                  </TableCell>
                  <TableCell
                    sx={{
                      fontSize: "12px",
                      padding: "0px",
                      textAlign: "center",
                    }}
                  >
                    {row.CargoCapT}
                  </TableCell>
                  <TableCell
                    sx={{
                      fontSize: "12px",
                      padding: "0px",
                      textAlign: "center",
                    }}
                  >
                    {row.paxLF}%
                  </TableCell>
                  <TableCell
                    sx={{
                      fontSize: "12px",
                      padding: "0px",
                      textAlign: "center",
                    }}
                  >
                    {row.cargoLF}%
                  </TableCell>
                  <TableCell
                    sx={{
                      whiteSpace: "nowrap",
                      fontSize: "12px",
                      padding: "0px",
                      textAlign: "center",
                    }}
                  >
                    {moment(row.fromDt).format("DD-MMM-YY")}
                  </TableCell>
                  <TableCell
                    sx={{
                      whiteSpace: "nowrap",
                      fontSize: "12px",
                      padding: "0px",
                      textAlign: "center",
                    }}
                  >
                    {moment(row.toDt).format("DD-MMM-YY")}
                  </TableCell>
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </Stack>
      <Stack direction="row" justifyContent="end">
        <Pagination
          count={Math.ceil(sectorsTableData.length / RowsPerPage)}
          page={currentPage}
          onChange={handlePageChange}
          color="primary"
        />
      </Stack>
      <ToastContainer />
    </Stack>
  );
};

export default SectorsTable;
