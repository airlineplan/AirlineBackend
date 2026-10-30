const multer = require("multer");
const path = require("node:path");
const fs = require("node:fs");
const crypto = require("node:crypto");
const directory = path.resolve(__dirname, "../private/uploads");
function privateUpload(kind) {
  const extensions = kind === "invoice" ? [".pdf"] : [".xlsx", ".xls"];
  const handler = multer({
    storage: multer.diskStorage({
      destination(req, file, callback) { fs.mkdir(directory, { recursive: true }, (error) => callback(error, directory)); },
      filename(req, file, callback) { callback(null, `${crypto.randomUUID()}${path.extname(file.originalname).toLowerCase()}`); },
    }),
    limits: { fileSize: 10 * 1024 * 1024, files: 1 },
    fileFilter(req, file, callback) { callback(extensions.includes(path.extname(file.originalname).toLowerCase()) ? null : new Error(`Upload a ${extensions.join(" or ")} file.`), true); },
  }).single("file");
  return (req, res, next) => handler(req, res, (error) => error
    ? res.status(error.code === "LIMIT_FILE_SIZE" ? 413 : 400).json({ message: error.message }) : next());
}
module.exports = { privateUpload, directory };
