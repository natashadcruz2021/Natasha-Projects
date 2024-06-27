// controllers/configController.js

const Config = require("../models/configModel");
const Module = require("../models/moduleModel");

// Create a new config
const createConfig = async (req, res) => {
  const { YucollectID, supplierID, lenderID, modules } = req.body;

  try {
    // Create the modules
    const moduleObjects = await Module.insertMany(modules);

    // Create the config
    const newConfig = new Config({
      YucollectID,
      supplierID,
      lenderID,
      modules: moduleObjects.map((module) => module._id),
    });

    await newConfig.save();

    res.status(201).json(newConfig);
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
};

// Get all configs
const getConfigs = async (req, res) => {
  try {
    const configs = await Config.find().populate("modules");
    res.status(200).json(configs);
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
};

// Get a single config
const getConfig = async (req, res) => {
  try {
    const config = await Config.findById(req.params.id).populate("modules");
    if (!config) {
      return res.status(404).json({ message: "Config not found" });
    }
    res.status(200).json(config);
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
};

// Update a config
const updateConfig = async (req, res) => {
  const { id } = req.params;
  const updateData = req.body;

  try {
    const updatedConfig = await Config.findByIdAndUpdate(id, updateData, {
      new: true, // Return the updated document
      runValidators: true, // Run schema validation
    });

    if (!updatedConfig) {
      return res.status(404).json({ message: "Config not found" });
    }

    res.status(200).json(updatedConfig);
  } catch (error) {
    res.status(500).json({ message: error.message });
  }
};

module.exports = { createConfig, getConfigs, getConfig, updateConfig };
