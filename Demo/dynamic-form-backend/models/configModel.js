// models/configModel.js

const mongoose = require("mongoose");

const configSchema = new mongoose.Schema({
  YucollectID: { type: String, required: true },
  supplierID: { type: String, required: true },
  lenderID: { type: String, required: true },
  modules: [{ type: mongoose.Schema.Types.ObjectId, ref: "Module" }],
});

const Config = mongoose.model("Config", configSchema);

module.exports = Config;
