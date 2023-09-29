const { Point, HttpError } = require("@influxdata/influxdb-client");
const User = require("../models/User");
const Chart = require("../models/Chart");
const { influxDb } = require("../config");
const WebSocket = require("../index");
const connectedClients = new Map();
const intervals = new Map();

async function writeDataToInflux(client, org, bucket, runzid, headers) {
  headers.map(async (ele) => {
    const writeApi = client.getWriteApi(org, bucket);
    const point1 = new Point(runzid)
      .tag("title", ele)
      .floatField("value", 20 + Math.round(100 * Math.random()) / 10);
    writeApi.writePoint(point1);
    await writeApi.close();
  });
}

const startConnect = async (runzid, valuesHeaders) => {
  await WebSocket.wssInstancePromise;
  const wss = await WebSocket.wssInstancePromise;
  const { client, org, bucket } = await influxDb();
  try {
    wss.on("connection", async (ws) => {
      connectedClients.set(runzid, ws);
      const intervalId = setInterval(async () => {
        await writeDataToInflux(client, org, bucket, runzid, valuesHeaders);
        const queryApi = client.getQueryApi(org);
        const fluxQuery = `from(bucket:"my-bucket") |> range(start: -1d)|> last()`;
        queryApi.queryRows(fluxQuery, {
          next(row, tableMeta) {
            const o = tableMeta.toObject(row);
            ws.send(JSON.stringify(o, null, 2));
          },
          error(error) {
            console.error(error);
            console.log('\\nFinished ERROR');
          },
          complete() {
            console.log('\\nFinished SUCCESS');
          },
        });
      }, 6000);
      intervals.set(runzid, intervalId);
    });
  } catch (e) {
    console.error(e);
    if (e instanceof HttpError && e.statusCode === 401) {
      console.log("Run ./onboarding.js to setup a new InfluxDB database.");
    }
    console.log("\nFinished ERROR");
  }
};

const closeConnect = (runzid) => {
  if (intervals.has(runzid)) {
    clearInterval(intervals.get(runzid));
    intervals.delete(runzid);
  }
  connectedClients.delete(runzid);
};

const createChart = async (req, res) => {
  try {
    const { runzId, values } = req.body;
    await startConnect(runzId, values);
    return res.status(200).json({ message: "Data streaming started" });
  } catch (error) {
    console.log(error);
    return res.status(500).json({ error: "Server error. Please try again" });
  }
};

const closeRunzChart = async(req, res)=>{
  try {
    const { runzId } = req.params;
    closeConnect(runzId)
    return res.status(200).json({ message: "Data streaming closed" });
  } catch (error) {
    console.log(error);
    return res.status(500).json({ error: "Server error. Please try again" });
  }
}

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
  closeRunzChart,
  listCharts,
  readInflux,
};
