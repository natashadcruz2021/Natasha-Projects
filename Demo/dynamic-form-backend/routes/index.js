const express = require("express");
const { formRouter } = require("./formRoutes");
const { configRouter } = require("./configRoutes");

const apiRouter = express.Router();

apiRouter.use("/forms", formRouter);
apiRouter.use("/config", configRouter);

module.exports = {
  apiRouter,
};
