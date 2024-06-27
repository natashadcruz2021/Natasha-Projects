const express = require("express");
const multer = require("multer");
const { submitForm } = require("../controller/formController");

const formRouter = express.Router();

// Set up multer for file handling
const storage = multer.memoryStorage();
const upload = multer({ storage: storage });

formRouter.post("/submit", upload.any(), submitForm);

module.exports = { formRouter };
