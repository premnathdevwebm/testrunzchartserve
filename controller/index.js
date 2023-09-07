const { Point } = require("@influxdata/influxdb3-client");
const User = require("../models/User");
const Chart = require("../models/Chart");
const { influxDb } = require("../config");
const wssInstance = require("../index");

const createChart = async (req, res) => {
  try {
    // const { client, database } = await influxDb();
    if (wssInstance) {

      wssInstance.on('connection', (ws) => {
        
      });

      res.status(200).json({ error: "Connection  established" });
    } else {
      res.status(500).json({ error: "Connection not established" });
    }
    return res.status(200).json({});
  } catch (error) {
    console.log(error);
    return res.status(500).json({ error: "Server error. Please try again" });
  }
};
const listCharts = async (req, res) => {
  try {
    let temp = await User.findOne({ userId: req.user.userId }).lean();
    temp = temp.chartIds.map((ele) => ele.toString());
    return res.status(200).json(temp);
  } catch (error) {
    return res.status(500).json({ error: "Server error. Please try again" });
  }
};
const readInflux = async (req, res) => {
  try {
   
    return res.status(200).json({});
  } catch (error) {
    return res.status(500).json({ error: "Server error. Please try again" });
  }
};
module.exports = {
  createChart,
  listCharts,
  readInflux,
};
