import React, { useState } from "react";
import {
  Table,
  TableHead,
  TableRow,
  TableCell,
  TableBody,
  Stack,
  Typography,
  TextField,
  Button,
  Menu,
  Pagination,
  Dialog,
} from "@mui/material";
import IconButton from "@mui/material/IconButton";
import DeleteIcon from "@mui/icons-material/Delete";
import Checkbox from "@mui/material/Checkbox";
import ArrowUpwardIcon from "@mui/icons-material/ArrowUpward";
import ArrowDownwardIcon from "@mui/icons-material/ArrowDownward";
import AddIcon from "@mui/icons-material/Add";
import DriveFolderUploadOutlinedIcon from "@mui/icons-material/DriveFolderUploadOutlined";
import CloseIcon from "@mui/icons-material/Close";
import axios from "axios";
import moment from "moment";
import { useEffect } from "react";
import UpdatePopUp from "./UpdatePopUp";
import { ToastContainer, toast } from "react-toastify";
import "react-toastify/dist/ReactToastify.css";
import CopyRow from "./CopyRow";
import AddNetwork from "./AddNetwork";
import { number } from "yup";
import LoadingButton from "@mui/lab/LoadingButton";

const label = { inputProps: { "aria-label": "Checkbox demo" } };

const RowsPerPage = 8;

const NetworkTable = () => {
  const [anchorEl, setAnchorEl] = useState(null);
  const [addbutton, setAddbutton] = useState(null);
  const [openUploadSched, setOpenUploadSched] = useState(false);
  const [flight, setFlight] = useState("");
  const [depStn, setDepStn] = useState("");
  const [std, setStd] = useState("");
  const [bt, setBt] = useState("");
  const [sta, setSta] = useState("");
  const [arrStn, setArrStn] = useState("");
  const [variant, setVariant] = useState("");
  const [effFromDt, setEffFromDt] = useState("");
  const [effToDt, setEffToDt] = useState("");
  const [dow, setDow] = useState("");
  const [domINTL, setDomIntl] = useState("");
  const [userTag1, setUserTag1] = useState("");
  const [userTag2, setUserTag2] = useState("");
  const [remarks1, setRemarks1] = useState("");
  const [remarks2, setRemarks2] = useState("");
  const [selectedFile, setSelectedFile] = useState(null);
  const [networkTableData, setNetworkTableData] = useState([]);
  const [checkedRows, setCheckedRows] = useState([]);
  const [currentPage, setCurrentPage] = useState(1);
  const [arrow, setArrow] = useState({ column: "", direction: "Up" });
  // .........Arrow Direction..........
  const [arrowDirection, setArrowDirection] = useState(true);
  const [loading, setLoading] = useState(false);
  const [add, setAdd] = useState(true);
  const [selectAll, setSelectAll] = useState(false);
  const [selectedRows, setSelectedRows] = useState([]);

  const sortedData = () => {
    if (!arrow.column) return networkTableData;

    const sorted = [...networkTableData].sort((a, b) => {
      const colA = a[arrow.column];
      const colB = b[arrow.column];

      if (arrow.direction === "Up") {
        return colA.localeCompare(colB);
      } else {
        return colB.localeCompare(colA);
      }
    });
    console.log(sorted, "sorted");

    return sorted;
  };

  var filteredIds = networkTableData
    .filter(
      (row) =>
        (row?.flight || "")?.toLowerCase().includes(flight?.toLowerCase()) &&
        (row?.depStn || "")?.toLowerCase()?.includes(depStn?.toLowerCase()) &&
        (row?.std || "")?.toLowerCase()?.includes(std?.toLowerCase()) &&
        (row?.bt || "")?.toLowerCase()?.includes(bt?.toLowerCase()) &&
        (row?.sta || "")?.toLowerCase()?.includes(sta?.toLowerCase()) &&
        (row?.arrStn || "")?.toLowerCase()?.includes(arrStn?.toLowerCase()) &&
        (row?.variant || "")?.toLowerCase()?.includes(variant?.toLowerCase()) &&
        (moment(row.effFromDt).format("DD-MMM-YY") || "")
          ?.toLowerCase()
          ?.includes(effFromDt?.toLowerCase()) &&
        (moment(row?.effToDt).format("DD-MMM-YY") || "")
          ?.toLowerCase()
          ?.includes(effToDt?.toLowerCase()) &&
        (row?.dow || "")?.toLowerCase()?.includes(dow?.toLowerCase()) &&
        (row?.domINTL || "")?.toLowerCase()?.includes(domINTL?.toLowerCase()) &&
        (row?.userTag1 || "")
          ?.toLowerCase()
          ?.includes(userTag1?.toLowerCase()) &&
        (row?.userTag2 || "")
          ?.toLowerCase()
          ?.includes(userTag2?.toLowerCase()) &&
        (row?.remarks1 || "")
          ?.toLowerCase()
          ?.includes(remarks1?.toLowerCase()) &&
        (row?.remarks2 || "")?.toLowerCase()?.includes(remarks2?.toLowerCase())
    )
    .map((row) => row._id);

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

  const handleArrow = (columnName) => {
    setArrow((prevArrow) => ({
      column: columnName,
      direction:
        prevArrow.column === columnName && prevArrow.direction === "Up"
          ? "Down"
          : "Up",
    }));
  };

  // ....................Arrow Direction Handler..........................
  const handleArrowDirection = () => {
    setArrowDirection(!arrowDirection);
  };

  const open = Boolean(anchorEl);

  const handleClick = (event) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const openadd = Boolean(addbutton);

  const handleAddClick = (event) => {
    setAddbutton(event.currentTarget);
  };
  const handleAddClose = () => {
    if (add) {
      setAddbutton(null);
    }
  };

  // .............................FilterHandlerFunctions.............................

  const handleFlight = (event) => {
    setFlight(event.target.value);
  };
  const handleDepStn = (event) => {
    setDepStn(event.target.value);
  };
  const handleSTD = (event) => {
    setStd(event.target.value);
  };
  const handleBT = (event) => {
    setBt(event.target.value);
  };
  const handleSTA = (event) => {
    setSta(event.target.value);
  };
  const handleArrStn = (event) => {
    setArrStn(event.target.value);
  };
  const handleVariant = (event) => {
    setVariant(event.target.value);
  };
  const handleEffFromDt = (event) => {
    setEffFromDt(event.target.value);
  };
  const handleEffToDt = (event) => {
    setEffToDt(event.target.value);
  };
  const handleDow = (event) => {
    setDow(event.target.value);
  };
  const handleDomIntl = (event) => {
    setDomIntl(event.target.value.toLowerCase());
  };
  const handleUserTag1 = (event) => {
    setUserTag1(event.target.value);
  };
  const handleUserTag2 = (event) => {
    setUserTag2(event.target.value);
  };
  const handleRemarks1 = (event) => {
    setRemarks1(event.target.value);
  };
  const handleRemarks2 = (event) => {
    setRemarks2(event.target.value);
  };

  const handleFileChange = (event) => {
    setSelectedFile(event.target.files[0]);
  };
  const handleFileUpload = () => {
    const accessToken = localStorage.getItem("accessToken");
    const url = "https://airlineplan.com/importUser";

    const formData = new FormData();
    formData.append("file", selectedFile);
    setLoading(true);

    axios
      .post(url, formData, {
        headers: {
          "Content-Type": "multipart/form-data",
          "x-access-token": accessToken,
        },
      })
      .then((response) => {
        // createConnections();
        setLoading(false);

        if (response.data.skippedRows && response.data.skippedRows.length > 0) {
          response.data.skippedRows.forEach((skippedRow) => {
            if (skippedRow.error === "Invalid flight number") {
              toast.error("Flight number is invalid for a row.");
            } else if (skippedRow.error === "Invalid departure station") {
              toast.error("Departure station is invalid for a row.");
            } else if (skippedRow.error === "Invalid Arr stn station") {
              toast.error("Arr station is invalid for a row.");
            } else if (skippedRow.error === "Invalid variant station") {
              toast.error("Invalid variant for a row.");
            } else if (skippedRow.error === "Invalid day of week") {
              toast.error("Day of week is invalid for a row.");
            } else if (skippedRow.error === "Data already exists") {
              toast.error("Data already exists for a row.");
            }
          });
        } else {
          // toast.success(response.data.msg);
          setOpenUploadSched(false);
          setTimeout(() => {
            window.location.reload();
          }, 2000);
        }
        if (response.data.status === 200) {
          toast.success(response.data.msg);
          setOpenUploadSched(false);
          setTimeout(() => {
            window.location.reload();
          }, 3000);
        }
      })

      .catch((error) => {
        console.error(error);
        setLoading(false);
        const errorMessage = error.response?.data?.msg || "An error occurred";
        toast.error(errorMessage);
      });
  };




  useEffect(() => {
    const fetchData = async () => {
      try {
        const accessToken = localStorage.getItem("accessToken");

        const response = await axios.get("https://airlineplan.com/get-data", {
          headers: {
            "x-access-token": accessToken,
          },
        });
        console.log(response.data, "response.data");
        setNetworkTableData(response.data);
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
      const accessToken = localStorage.getItem("accessToken");
      const response = await axios.delete(
        "https://airlineplan.com/delete",
        {
          data: { ids: checkedRows },       // ids in the body
          headers: { "x-access-token": accessToken }
        }
      );

      if (
        response.data &&
        response.data.message === "Data deleted successfully"
      ) {
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
        gap="15px"
        mt="15px"
      >
        <Stack direction="row" gap="16px" justifyContent="space-between">
          <Button
            variant="contained"
            startIcon={<DriveFolderUploadOutlinedIcon />}
            sx={{ textTransform: "capitalize", width: "fit-content" }}
            onClick={() => {
              setOpenUploadSched(true);
            }}
          >
            Upload Schedule
          </Button>
          <Stack>
            <Button
              variant="contained"
              sx={{ textTransform: "capitalize", width: "fit-content" }}
              startIcon={<AddIcon />}
              id="addbutton"
              aria-controls={openadd ? "addOptions" : undefined}
              aria-haspopup="true"
              aria-expanded={openadd ? "true" : undefined}
              onClick={handleAddClick}
            >
              Add
            </Button>
            <Menu
              id="addOptions"
              anchorEl={addbutton}
              open={openadd}
              onClose={handleAddClose}
              MenuListProps={{
                "aria-labelledby": "addbutton",
              }}
            >
              <CopyRow checkedRows={checkedRows} setAdd={setAdd} />
              <AddNetwork setAdd={setAdd} />
            </Menu>
          </Stack>

          {/* ............................Upload_Schedule_Modal........................ */}
          <Dialog
            open={openUploadSched}
            onClose={() => {
              setOpenUploadSched(false);
            }}
          >
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
                marginTop: "10px",
                marginLeft: "10px",
                marginRight: "10px",
              }}
            >
              <Typography>Add File</Typography>

              <CloseIcon
                sx={{ cursor: "pointer" }}
                onClick={() => {
                  setOpenUploadSched(false);
                  setLoading(false);
                }}
              />
            </div>
            <Stack padding={5}>
              <input type="file" onChange={handleFileChange} />
              <LoadingButton
                type="file"
                loading={loading}
                variant="contained"
                sx={{ marginTop: "50px" }}
                onClick={handleFileUpload}
              >
                <span>Submit</span>
              </LoadingButton>
            </Stack>
          </Dialog>
          <UpdatePopUp checkedRows={checkedRows} />
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
      <Stack gap="15px">
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
                  placeholder="Flight"
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "12px" },
                  }}
                  onChange={handleFlight}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="Dep Stn"
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "12px" },
                  }}
                  onChange={handleDepStn}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="STD"
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "12px" },
                  }}
                  onChange={handleSTD}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="BT"
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "12px" },
                  }}
                  onChange={handleBT}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="STA"
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "12px" },
                  }}
                  onChange={handleSTA}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="Arr Stn"
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "12px" },
                  }}
                  onChange={handleArrStn}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="Variant"
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "12px" },
                  }}
                  onChange={handleVariant}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="Eff from Dt"
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "12px" },
                  }}
                  onChange={handleEffFromDt}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="Eff to Dt"
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "12px" },
                  }}
                  onChange={handleEffToDt}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="Dow"
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "12px" },
                  }}
                  onChange={handleDow}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="Dom / INTL"
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "12px" },
                  }}
                  onChange={handleDomIntl}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="User Tag 1"
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "12px" },
                  }}
                  onChange={handleUserTag1}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="User Tag 2"
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "12px" },
                  }}
                  onChange={handleUserTag2}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="Remarks1"
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "12px" },
                  }}
                  onChange={handleRemarks1}
                />
              </TableCell>
              <TableCell sx={{ paddingX: "4px" }}>
                <TextField
                  variant="outlined"
                  size="small"
                  placeholder="Remarks2"
                  sx={{
                    minWidth: "10px",
                    fontSize: "10px",
                    "& .MuiOutlinedInput-input": { fontSize: "12px" },
                  }}
                  onChange={handleRemarks2}
                />
              </TableCell>
            </TableRow>
          </TableHead>
          <TableHead>
            <TableRow sx={{ bgcolor: "#F5F5F5" }}>
              <TableCell
                sx={{
                  whiteSpace: "nowrap",
                  fontWeight: "bold",
                  padding: "0px",
                  textAlign: "center",
                  fontSize: "12px",
                }}
              >
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
                  padding: "0px",
                  textAlign: "center",
                  fontSize: "12px",
                }}
              >
                Flight #
                <IconButton
                  onClick={() => {
                    handleArrow("flight");
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
                Dep Stn
                <IconButton
                  onClick={() => {
                    handleArrow("depStn");
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
                STD (LT)
                <IconButton
                  onClick={() => {
                    handleArrow("std");
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
                BT
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
                  padding: "0px",
                  textAlign: "center",
                  fontSize: "12px",
                }}
              >
                STA(LT)
                <IconButton
                  onClick={() => {
                    handleArrow("sta");
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
                Arr Stn
                <IconButton
                  onClick={() => {
                    handleArrow("arrStn");
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
                  padding: "0px",
                  textAlign: "center",
                  fontSize: "12px",
                }}
              >
                Eff from Dt
                <IconButton
                  onClick={() => {
                    handleArrow("effFromDt");
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
                Eff to Dt
                <IconButton
                  onClick={() => {
                    handleArrow("effToDt");
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
                DoW
                <IconButton
                  onClick={() => {
                    handleArrow("dow");
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
                Dom / INTL
                <IconButton
                  onClick={() => {
                    handleArrow("domINTL");
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
                User Tag 1
                <IconButton
                  onClick={() => {
                    handleArrow("userTag1");
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
                User Tag 2
                <IconButton
                  onClick={() => {
                    handleArrow("userTag2");
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
                Remarks 1
                <IconButton
                  onClick={() => {
                    handleArrow("remarks1");
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
                Remarks 2
                <IconButton
                  onClick={() => {
                    handleArrow("remarks2");
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
                (row) =>
                  (row?.flight || "")
                    ?.toLowerCase()
                    .includes(flight?.toLowerCase()) &&
                  (row?.depStn || "")
                    ?.toLowerCase()
                    ?.includes(depStn?.toLowerCase()) &&
                  (row?.std || "")
                    ?.toLowerCase()
                    ?.includes(std?.toLowerCase()) &&
                  (row?.bt || "")?.toLowerCase()?.includes(bt?.toLowerCase()) &&
                  (row?.sta || "")
                    ?.toLowerCase()
                    ?.includes(sta?.toLowerCase()) &&
                  (row?.arrStn || "")
                    ?.toLowerCase()
                    ?.includes(arrStn?.toLowerCase()) &&
                  (row?.variant || "")
                    ?.toLowerCase()
                    ?.includes(variant?.toLowerCase()) &&
                  (moment(row.effFromDt).format("DD-MMM-YY") || "")
                    ?.toLowerCase()
                    ?.includes(effFromDt?.toLowerCase()) &&
                  (moment(row?.effToDt).format("DD-MMM-YY") || "")
                    ?.toLowerCase()
                    ?.includes(effToDt?.toLowerCase()) &&
                  (row?.dow || "")
                    ?.toLowerCase()
                    ?.includes(dow?.toLowerCase()) &&
                  (row?.domINTL || "")
                    ?.toLowerCase()
                    ?.includes(domINTL?.toLowerCase()) &&
                  (row?.userTag1 || "")
                    ?.toLowerCase()
                    ?.includes(userTag1?.toLowerCase()) &&
                  (row?.userTag2 || "")
                    ?.toLowerCase()
                    ?.includes(userTag2?.toLowerCase()) &&
                  (row?.remarks1 || "")
                    ?.toLowerCase()
                    ?.includes(remarks1?.toLowerCase()) &&
                  (row?.remarks2 || "")
                    ?.toLowerCase()
                    ?.includes(remarks2?.toLowerCase())
              )
              .slice(startIndex, endIndex)
              .map((row, index) => (
                <TableRow key={index} sx={{ backgroundColor: index % 2 !== 0 ? '#f0f0f0' : 'inherit' }}>
                  <TableCell
                    sx={{
                      padding: "0px",
                      textAlign: "center",
                      fontSize: "12px",
                    }}
                  >
                    <Checkbox
                      checked={checkedRows.includes(row._id)}
                      onChange={(event) => handleCheckboxChange(event, row._id)}
                    />
                  </TableCell>
                  <TableCell
                    sx={{
                      padding: "0px",
                      textAlign: "center",
                      fontSize: "12px",
                    }}
                  >
                    {row.flight}
                  </TableCell>
                  <TableCell
                    sx={{
                      padding: "0px",
                      textAlign: "center",
                      fontSize: "12px",
                    }}
                  >
                    {row.depStn}
                  </TableCell>
                  <TableCell
                    sx={{
                      padding: "0px",
                      textAlign: "center",
                      fontSize: "12px",
                    }}
                  >
                    {row.std}
                  </TableCell>
                  <TableCell
                    sx={{
                      padding: "0px",
                      textAlign: "center",
                      fontSize: "12px",
                    }}
                  >
                    {row?.bt}
                  </TableCell>
                  <TableCell
                    sx={{
                      padding: "0px",
                      textAlign: "center",
                      fontSize: "12px",
                    }}
                  >
                    {row.sta}
                  </TableCell>
                  <TableCell
                    sx={{
                      padding: "0px",
                      textAlign: "center",
                      fontSize: "12px",
                    }}
                  >
                    {row.arrStn}
                  </TableCell>
                  <TableCell
                    sx={{
                      padding: "0px",
                      textAlign: "center",
                      fontSize: "12px",
                    }}
                  >
                    {row.variant}
                  </TableCell>
                  <TableCell
                    sx={{
                      padding: "0px",
                      textAlign: "center",
                      fontSize: "12px",
                    }}
                  >
                    {moment(row.effFromDt).format("DD-MMM-YY")}
                  </TableCell>
                  <TableCell
                    sx={{
                      padding: "0px",
                      textAlign: "center",
                      fontSize: "12px",
                    }}
                  >
                    {/* {row.effToDt} */}
                    {moment(row.effToDt).format("DD-MMM-YY")}
                  </TableCell>
                  <TableCell
                    sx={{
                      padding: "0px",
                      textAlign: "center",
                      fontSize: "12px",
                    }}
                  >
                    {row.dow}
                  </TableCell>
                  <TableCell
                    sx={{
                      padding: "0px",
                      textAlign: "center",
                      fontSize: "12px",
                    }}
                  >
                    {row.domINTL}
                  </TableCell>
                  <TableCell
                    sx={{
                      padding: "0px",
                      textAlign: "center",
                      fontSize: "12px",
                    }}
                  >
                    {row.userTag1}
                  </TableCell>
                  <TableCell
                    sx={{
                      padding: "0px",
                      textAlign: "center",
                      fontSize: "12px",
                    }}
                  >
                    {row.userTag2}
                  </TableCell>
                  <TableCell
                    sx={{
                      padding: "0px",
                      textAlign: "center",
                      fontSize: "12px",
                    }}
                  >
                    {row.remarks1}
                  </TableCell>
                  <TableCell
                    sx={{
                      padding: "0px",
                      textAlign: "center",
                      fontSize: "12px",
                    }}
                  >
                    {row.remarks2}
                  </TableCell>
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </Stack>
      <Stack direction="row" justifyContent="end">
        {/* <Pagination count={10} color="primary" /> */}

        <Pagination
          count={Math.ceil(networkTableData.length / RowsPerPage)}
          page={currentPage}
          onChange={handlePageChange}
          color="primary"
        />
      </Stack>
      <ToastContainer />
    </Stack>
  );
};

export default NetworkTable;
