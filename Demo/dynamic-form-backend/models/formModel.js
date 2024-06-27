const mongoose = require("mongoose");

// Define a schema and model
const dynamicFormSchema = new mongoose.Schema({}, { strict: false });
const DynamicFormData = mongoose.model("DynamicFormData", dynamicFormSchema);

module.exports = DynamicFormData;
