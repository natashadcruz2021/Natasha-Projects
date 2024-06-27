const express = require("express");
const multer = require("multer");
const {
  getConfig,
  getConfigs,
  createConfig,
} = require("../controller/configController");

const configRouter = express.Router();

configRouter.post("/config", createConfig);
configRouter.get("/configs", getConfigs);
configRouter.get("/config/:id", getConfig);
configRouter.put("/config/:id", updateConfig);

module.exports = { configRouter };
