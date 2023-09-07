require("dotenv").config();
const express = require("express");
const compression = require("compression");
const cors = require("cors");
const WebSocket = require("ws");

const router = require("./routes");
const { db, connectMessageQue } = require("./config");
const {
  errorLogger,
  errorResponder,
  invalidPathHandler,
} = require("./middleware");

function createWebSocketServer() {
  return new Promise(async (resolve, reject) => {
    try {
      const app = express();
      app.use(cors());
      app.use(compression());
      app.use(express.json({ limit: "100mb" }));
      app.use(express.urlencoded({ extended: true }));

      app.use(router);

      app.use(errorLogger);
      app.use(errorResponder);
      app.use(invalidPathHandler);
      app.get("/error", (req, res) => {
        res.send("The endpoint you are trying to reach does not exist.");
      });

      const server = app.listen(process.env.PORT, () => {
        console.log(`Chart Service running on ${process.env.PORT}`);
      });

      const wss = new WebSocket.Server({ server });

      wss.on("listening", () => {
        console.log("WebSocket Server is listening.");
        resolve(wss);
      });

      await connectMessageQue();
    } catch (error) {
      console.error("Error initializing WebSocket Server:", error);
      reject(error);
    }
  });
}

module.exports.wssInstancePromise = db()
  .then(async () => {
    const wss = await createWebSocketServer();
    return new Promise((resolve, reject) => resolve(wss));
  })
  .catch((err) => {
    console.error(err);
  });

