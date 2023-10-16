const User = require("../models/User");
const { influxDb } = require("../config");
const WebSocket = require("../index");
const { from, interval, Subject } = require("rxjs");
const { map, switchMap, takeUntil } = require("rxjs/operators");
const connectedClients = new Map();

const startConnect = async (runzid) => {
  await WebSocket.wssInstancePromise;
  const wss = await WebSocket.wssInstancePromise;
  wss.on("connection", async (ws) => {
    const stopStream$ = new Subject();
    ws.on("message", async (message) => {
      const data = JSON.parse(message);
      switch (data.type) {
        case "start":
          try {
            const { client, org, bucket } = await influxDb();
            const queryApi = client.getQueryApi(org);
            const fluxQuery = `
                from(bucket:"${bucket}")
                  |> range(start: -1m)
                  |> filter(fn: (r) => r._measurement == "${runzid}")
                  |> last()
              `;
            interval(1000)
              .pipe(
                switchMap(() =>
                  from(queryApi.rows(fluxQuery)).pipe(
                    map(({ values, tableMeta }) => tableMeta.toObject(values))
                  )
                ),
                takeUntil(stopStream$)
              )
              .subscribe({
                next(o) {
                  ws.send(JSON.stringify(o, null, 2));
                },
                error(e) {
                  console.error(e);
                  console.log("\nFinished ERROR");
                },
                complete() {
                  console.log("\nFinished SUCCESS");
                },
              });
            connectedClients.set(runzid, {
              ws,
            });
          } catch (error) {
            console.error("ERROR", error);
          }
          break;
        case "stop":
          const clientInfo = connectedClients.get(runzid);
          if (clientInfo) {
            const { ws } = clientInfo;
            ws.send(JSON.stringify({ message: "Data streaming stopped" }));
            ws.close();
            connectedClients.delete(runzid);
            stopStream$.next();
          }
          break;
        default:
          console.error("Unknown message type:", data.type);
      }
    });
  });
};

const createChart = async (req, res) => {
  try {
    const { runzId } = req.body;
    await startConnect(runzId);
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
