const mongoose = require("mongoose");
const { Schema } = mongoose;

const ChartSchema = new Schema(
  {
    runzId: String,
    type: String,
    startTime: String,
    endTime: String,
  }
);

module.exports = Chart = mongoose.model("chart", ChartSchema);