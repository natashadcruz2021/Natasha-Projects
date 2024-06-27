// models/moduleModel.js

const mongoose = require("mongoose");

const moduleSchema = new mongoose.Schema({
  name: { type: String, required: true },
  requiredFields: [String],
});

const Module = mongoose.model("Module", moduleSchema);

module.exports = Module;
