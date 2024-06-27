const DynamicFormData = require("../models/formModel");

const submitForm = async (req, res) => {
  const formData = req.body;

  // Handle file uploads
  req.files &&
    req.files.forEach((file) => {
      formData[file.fieldname] = file.buffer.toString("base64"); // Example of how to handle file data
    });

  const newFormData = new DynamicFormData(formData);
  try {
    await newFormData.save();
    res.status(200).send("Form data saved successfully");
  } catch (error) {
    res.status(500).send("Error saving form data");
  }
};

module.exports = {
  submitForm,
};
