const { Point, HttpError } = require("@influxdata/influxdb-client");
const User = require("../models/User");
const Chart = require("../models/Chart");
const { influxDb } = require("../config");
const WebSocket = require("../index");
const connectedClients = new Map();
const intervals = new Map();

async function writeDataToInflux(client, database, runzid, headers) {
  /* headers.map(async (ele) => {
    const point = new Point();
    const pointTemplate = Object.freeze(
      point.measurement("stat").tag("unit", ele)
    );

    const p2 = pointTemplate
      .floatField("avg", 24.5)
      .floatField("max", 45.0);

    await client.write(p2, database);
  });
  closeConnect(runzid); */
}

const startConnect = async (runzid, valuesHeaders) => {
  await WebSocket.wssInstancePromise;
  const wss = await WebSocket.wssInstancePromise;
  wss.on("connection", async (ws) => {
    connectedClients.set(runzid, ws);
    const { client, org, bucket } = await influxDb();
    const writeApi = client.getWriteApi(org, bucket);
    const point1 = new Point("temperature")
      .tag("example", "write.ts")
      .floatField("value", 20 + Math.round(100 * Math.random()) / 10);
    writeApi.writePoint(point1);
    try {
      await writeApi.close();
      console.log("FINISHED ... now try ./query.ts");
      const queryApi = client.getQueryApi(org);
      const fluxQuery = `from(bucket:"my-bucket") |> range(start: -1d) |> filter(fn: (r) => r._measurement == "temperature")`;
      for await (const { values, tableMeta } of queryApi.iterateRows(
        fluxQuery
      )) {
        const o = tableMeta.toObject(values)
        ws.send(
          `${o._time} ${o._measurement} in '${o.location}' (${o.example}): ${o._field}=${o._value}`
        )
      }
    } catch (e) {
      console.error(e);
      if (e instanceof HttpError && e.statusCode === 401) {
        console.log("Run ./onboarding.js to setup a new InfluxDB database.");
      }
      console.log("\nFinished ERROR");
    }
    /*  const intervalId = setInterval(async () => {
      await writeDataToInflux(client, database, runzid, valuesHeaders);
    }, 5000);
    intervals.set(runzid, intervalId); */
  });
};

const closeConnect = (runzid) => {
  if (intervals.has(runzid)) {
    clearInterval(intervals.get(runzid));
    intervals.delete(runzid);
  }
  connectedClients.delete(runzid);
};

const createChart = async (req, res, next) => {
  try {
    const { runzId, values } = req.body;
    await startConnect(runzId, values);
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
