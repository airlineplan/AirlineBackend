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

const label = { inputProps: { "aria-label": "Checkbox demo" } };

const RowsPerPage = 8;

const NetworkTable = () => {
  const [anchorEl, setAnchorEl] = useState(null);
  const [addbutton, setAddbutton] = useState(null);
  const [openUploadSched, setOpenUploadSched] = useState(false);
  const [openCopyModal, setOpenCopyModal] = useState(false);
  const [openModal, setOpenModal] = useState(false);
  const [openUpdate, setOpenUpdate] = useState(false);
  // const [arrow, setArrow] = useState("Up");
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
  const [data, setData] = useState([]);
  const [networkTableData, setNetworkTableData] = useState([]);
  const [checkedRows, setCheckedRows] = useState("");
  const [deletedData, setDeletedData] = useState(null);
  const [product, setProduct] = useState("");
  const [currentPage, setCurrentPage] = useState(1);
  const [arrow, setArrow] = useState({ column: null, direction: "Up" });

  // const handleCheckboxChange = (event, row) => {
  //   const { checked } = event.target;
  //   if (checked) {
  //     setCheckedRows((prevCheckedRows) => [...prevCheckedRows, row._id]);
  //   } else {
  //     setCheckedRows((prevCheckedRows) =>
  //       prevCheckedRows.filter((id) => id !== row._id)
  //     );
  //   }
  // };

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

    return sorted;
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

  const handleCheckboxChange = (event, row) => {
    if (event.target.checked) {
      setCheckedRows(row);
    } else {
      setCheckedRows(null);
    }
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
    setAddbutton(null);
  };
  const handleUpdateOpen = () => {
    setOpenUpdate(true);
  };

  const handleUpdateClose = () => {
    setOpenUpdate(false);
  };

  // const handleArrow = () => {
  //   if (arrow === "Up") {
  //     setArrow("Down");
  //   } else {
  //     setArrow("Up");
  //   }
  // };

  const handleFileSelect = (event) => {
    const file = event.target.files[0];
    console.log("Selected file:", file);
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
    const formData = new FormData();
    formData.append("file", selectedFile);

    axios
      .post("https://airlineplan.com/importUser", formData, {
        headers: {
          "Content-Type": "multipart/form-data",
        },
      })
      .then((response) => {
        toast.success(response.data.msg);
        setOpenUploadSched(false);
        setTimeout(() => {
          window.location.reload();
        }, 2000);
      })
      .catch((error) => {
        console.error(error);
        toast.error(response.data.msg);
      });
  };

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await axios.get("https://airlineplan.com/get-data");
        setNetworkTableData(response.data);
      } catch (error) {
        console.error("Error fetching data:", error);
      }
    };

    fetchData();
  }, []);

  const handleDeleteData = async (id) => {
    const isConfirmed = window.confirm(
      "Are you sure you want to delete this data?"
    );

    if (!isConfirmed) {
      return;
    }

    try {
      const response = await axios.delete(
        "https://airlineplan.com/delete",
        {
          data: { ids: checkedRows },       // ids in the body
          headers: { "x-access-token": accessToken }
        }
      );
      setDeletedData(response.data.data);
      toast.success("Delete Successfull");
      setTimeout(() => {
        window.location.reload();
      }, 2000);
    } catch (error) {
      toast.error(error);
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
              <CopyRow checkedRows={checkedRows} />
              <AddNetwork />
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
                onClick={() => setOpenUploadSched(false)}
              />
            </div>
            <Stack padding={5}>
              {/* <Button type='file' variant='contained'>Select</Button> */}

              <input type="file" onChange={handleFileChange} />
              {/* <Typography
                sx={{ fontSize: "10px", textAlign: "center", mt: "10px" }}
              >
                ! Choose you file
              </Typography> */}
              <Button
                type="file"
                variant="contained"
                sx={{ marginTop: "50px" }}
                onClick={handleFileUpload}
              >
                Submit
              </Button>
            </Stack>
          </Dialog>
          <UpdatePopUp checkedRows={checkedRows} />
        </Stack>
        <Stack>
          <Button
            variant="outlined"
            startIcon={<DeleteIcon />}
            onClick={() => handleDeleteData(checkedRows._id)}
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
                <Checkbox {...label} size="small" />
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
                <IconButton onClick={() => handleArrow('flight')}>
                  {arrow.column === "flight" ? (
                    arrow.direction === "Up" ? (
                      <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                    ) : (
                      <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                    )
                  ) : null}
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
                <IconButton onClick={() => handleArrow('depStn')}>
                  {arrow.column === 'depStn' ? (
                    arrow.direction === 'Up' ? (
                      <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                    ) : (
                      <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                    )
                  ) : null}
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
                <IconButton onClick={() => handleArrow('std')}>
                  {arrow.column === 'std' ? (
                    arrow.direction === 'Up' ? (
                      <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                    ) : (
                      <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                    )
                  ) : null}
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
                <IconButton onClick={() => handleArrow('bt')}>
                  {arrow.column === 'bt' ? (
                    arrow.direction === 'Up' ? (
                      <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                    ) : (
                      <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                    )
                  ) : null}
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
                <IconButton onClick={() => handleArrow('sta')}>
                  {arrow.column === 'sta' ? (
                    arrow.direction === 'Up' ? (
                      <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                    ) : (
                      <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                    )
                  ) : null}
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
                <IconButton onClick={() => handleArrow('arrStn')}>
                  {arrow.column === 'arrStn' ? (
                    arrow.direction === 'Up' ? (
                      <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                    ) : (
                      <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                    )
                  ) : null}
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
                <IconButton onClick={() => handleArrow('variant')}>
                  {arrow.column === 'variant' ? (
                    arrow.direction === 'Up' ? (
                      <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                    ) : (
                      <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                    )
                  ) : null}
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
                <IconButton onClick={() => handleArrow('effFromDt')}>
                  {arrow.column === 'effFromDt' ? (
                    arrow.direction === 'Up' ? (
                      <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                    ) : (
                      <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                    )
                  ) : null}
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
                <IconButton onClick={() => handleArrow('effToDt')}>
                  {arrow.column === 'effToDt' ? (
                    arrow.direction === 'Up' ? (
                      <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                    ) : (
                      <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                    )
                  ) : null}
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
                <IconButton onClick={() => handleArrow('dow')}>
                  {arrow.column === 'dow' ? (
                    arrow.direction === 'Up' ? (
                      <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                    ) : (
                      <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                    )
                  ) : null}
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
                <IconButton onClick={() => handleArrow('domINTL')}>
                  {arrow.column === 'domINTL' ? (
                    arrow.direction === 'Up' ? (
                      <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                    ) : (
                      <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                    )
                  ) : null}
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
                <IconButton onClick={() => handleArrow('userTag1')}>
                  {arrow.column === 'userTag1' ? (
                    arrow.direction === 'Up' ? (
                      <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                    ) : (
                      <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                    )
                  ) : null}
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
                <IconButton onClick={() => handleArrow('userTag2')}>
                  {arrow.column === 'userTag2' ? (
                    arrow.direction === 'Up' ? (
                      <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                    ) : (
                      <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                    )
                  ) : null}
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
                <IconButton onClick={() => handleArrow('remarks1')}>
                  {arrow.column === 'remarks1' ? (
                    arrow.direction === 'Up' ? (
                      <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                    ) : (
                      <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                    )
                  ) : null}
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
                <IconButton onClick={() => handleArrow('remarks2')}>
                  {arrow.column === 'remarks2' ? (
                    arrow.direction === 'Up' ? (
                      <ArrowUpwardIcon sx={{ fontSize: "16px" }} />
                    ) : (
                      <ArrowDownwardIcon sx={{ fontSize: "16px" }} />
                    )
                  ) : null}
                </IconButton>
              </TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {sortedData()
              .filter(
                (row) =>
                  (row.flight || "")
                    ?.toLowerCase()
                    .includes(flight?.toLowerCase()) &&
                  (row.depStn || "")
                    ?.toLowerCase()
                    ?.includes(depStn?.toLowerCase()) &&
                  (row.std || "")
                    ?.toLowerCase()
                    ?.includes(std?.toLowerCase()) &&
                  (row.bt || "")?.toLowerCase()?.includes(bt?.toLowerCase()) &&
                  (row.sta || "")
                    ?.toLowerCase()
                    ?.includes(sta?.toLowerCase()) &&
                  (row.arrStn || "")
                    ?.toLowerCase()
                    ?.includes(arrStn?.toLowerCase()) &&
                  (row.variant || "")
                    ?.toLowerCase()
                    ?.includes(variant?.toLowerCase()) &&
                  (row.effFromDt || "")
                    ?.toLowerCase()
                    ?.includes(effFromDt?.toLowerCase()) &&
                  (row.effToDt || "")
                    ?.toLowerCase()
                    ?.includes(effToDt?.toLowerCase()) &&
                  (row.dow || "")
                    ?.toLowerCase()
                    ?.includes(dow?.toLowerCase()) &&
                  (row.domINTL || "")
                    ?.toLowerCase()
                    ?.includes(domINTL?.toLowerCase()) &&
                  (row.userTag1 || "")
                    ?.toLowerCase()
                    ?.includes(userTag1?.toLowerCase()) &&
                  (row.userTag2 || "")
                    ?.toLowerCase()
                    ?.includes(userTag2?.toLowerCase()) &&
                  (row.remarks1 || "")
                    ?.toLowerCase()
                    ?.includes(remarks1?.toLowerCase()) &&
                  (row.remarks2 || "")
                    ?.toLowerCase()
                    ?.includes(remarks2?.toLowerCase())
              )
              .slice(startIndex, endIndex)
              .map((row, index) => (
                <TableRow key={index}>
                  <TableCell
                    sx={{
                      padding: "0px",
                      textAlign: "center",
                      fontSize: "12px",
                    }}
                  >
                    {/* <Checkbox
                      {...label}
                      size="small"
                      onChange={(event) => handleCheckboxChange(event, row)}
                    /> */}

                    <Checkbox
                      {...label}
                      size="small"
                      checked={checkedRows === row} // Check if the current row is the selected one
                      onChange={(event) => handleCheckboxChange(event, row)}
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
                    {/* {moment(row.bt, "HH:mm").format("hh:mm A")} */}
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

{
  /* <TableHead>
              <TableRow>
                <TableCell sx={{padding: '0px'}}><Typography sx={{ fontWeight: 'bold' }}>Sort By:-</Typography></TableCell>
                <TableCell sx={{padding: '0px'}}>
                  <TextField variant='outlined' size='small' placeholder='Flight' sx={{ minWidth: '10px',fontSize: '10px' }} />
                </TableCell>
                <TableCell sx={{padding: '0px'}}>
                  <TextField variant='outlined' size='small' placeholder='Dep Stn' sx={{ minWidth: '10px',fontSize: '10px' }} />
                </TableCell>
                <TableCell sx={{padding: '0px'}}>
                  <TextField variant='outlined' size='small' placeholder='STD' sx={{ minWidth: '10px',fontSize: '10px' }} />
                </TableCell>
                <TableCell sx={{padding: '0px'}}>
                  <TextField variant='outlined' size='small' placeholder='BT' sx={{ minWidth: '10px',fontSize: '10px' }} />
                </TableCell>
                <TableCell sx={{padding: '0px'}}>
                  <TextField variant='outlined' size='small' placeholder='STA' sx={{ minWidth: '10px',fontSize: '10px' }} />
                </TableCell>
                <TableCell sx={{padding: '0px'}}>
                  <TextField variant='outlined' size='small' placeholder='Arr Stn' sx={{ minWidth: '10px',fontSize: '10px' }} />
                </TableCell>
                <TableCell sx={{padding: '0px'}}>
                  <TextField variant='outlined' size='small' placeholder='Variant' sx={{ minWidth: '10px',fontSize: '10px' }} />
                </TableCell>
                <TableCell sx={{padding: '0px'}}>
                  <TextField variant='outlined' size='small' placeholder='Eff from Dt' sx={{ minWidth: '10px',fontSize: '10px' }} />
                </TableCell>
                <TableCell sx={{padding: '0px'}}>
                  <TextField variant='outlined' size='small' placeholder='Eff to Dt' sx={{ minWidth: '10px',fontSize: '10px' }} />
                </TableCell>
                <TableCell sx={{padding: '0px'}}>
                  <TextField variant='outlined' size='small' placeholder='Dow' sx={{ minWidth: '10px',fontSize: '10px' }} />
                </TableCell>
                <TableCell sx={{padding: '0px'}}>
                  <TextField variant='outlined' size='small' placeholder='Label1' sx={{ minWidth: '10px',fontSize: '10px' }} />
                </TableCell>
                <TableCell sx={{padding: '0px'}}>
                  <TextField variant='outlined' size='small' placeholder='Label2' sx={{ minWidth: '10px',fontSize: '10px' }} />
                </TableCell>
                <TableCell sx={{padding: '0px'}}>
                  <TextField variant='outlined' size='small' placeholder='Label3' sx={{ minWidth: '10px',fontSize: '10px' }} />
                </TableCell>
                <TableCell sx={{padding: '0px'}}>
                  <TextField variant='outlined' size='small' placeholder='Remarks1' sx={{ minWidth: '10px',fontSize: '10px' }} />
                </TableCell>
                <TableCell sx={{padding: '0px'}}>
                  <TextField variant='outlined' size='small' placeholder='Remarks2' sx={{ minWidth: '10px',fontSize: '10px' }} />
                </TableCell>
              </TableRow>
            </TableHead> */
}
