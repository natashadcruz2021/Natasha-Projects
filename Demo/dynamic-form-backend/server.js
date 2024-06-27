// backend/server.js
const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");
const routePath = require("./routes");

const connectDB = require("./config/db");

const app = express();
const PORT = 5000;

// Connect to MongoDB
connectDB();

// Middleware
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(cors());

// // Set up multer for file handling
// const storage = multer.memoryStorage();
// const upload = multer({ storage: storage });

// Routes
app.use("/api", routePath.apiRouter);

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});
