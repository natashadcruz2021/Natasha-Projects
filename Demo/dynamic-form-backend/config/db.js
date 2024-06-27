const mongoose = require("mongoose");

const connectDB = async () => {
  try {
    await mongoose
      .connect(
        "mongodb+srv://natashadcruz:BKjZNubbrAZlhj6D@mvpdemocluster.2fxjiot.mongodb.net/db"
      )
      .then(() => console.log("MongoDB connected"))
      .catch((error) => console.log("MongoDB connection error:", error));

    // const db = mongoose.connection;
    // db.on("error", console.error.bind(console, "connection error:"));
    // db.once("open", () => {
    //   console.log("Connected to MongoDB");
    // });
  } catch (error) {
    console.error("MongoDB connection error:", error.message);
    process.exit(1);
  }
};

module.exports = connectDB;
