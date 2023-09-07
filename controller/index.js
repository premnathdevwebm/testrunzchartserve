const { Point } = require("@influxdata/influxdb3-client");
const User = require("../models/User");
const Chart = require("../models/Chart");
const { influxDb } = require("../config");
const WebSocket = require("../index");
const connectedClients = new Map();

const startConnect = async (runzid) => {
  await WebSocket.wssInstancePromise;
  const wss = await WebSocket.wssInstancePromise;
  wss.on("connection", ws=>{
    connectedClients.set(runzid, ws);
  })
};

const closeConnect = (runzid)=>{
  connectedClients.delete(runzid);
}

const createChart = async (req, res, next) => {
  try {
    const {runzId} = req.body
    // const { client, database } = await influxDb();
    await startConnect(runzId);
    console.log(connectedClients);
    return res.status(200).json({ message: "Data streaming started" });
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
