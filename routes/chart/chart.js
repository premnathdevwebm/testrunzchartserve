const { Router } = require("express");

const { createChart, pauseChart, listCharts, realTimeChart, readInflux } = require("../../controller");
const { isAuthenticatedChart, commonRole } = require("../../middleware");

const router = new Router();

router.post("/chart", isAuthenticatedChart, commonRole, createChart);
router.patch("/chart", isAuthenticatedChart, commonRole, pauseChart);
router.get("/charts", isAuthenticatedChart, commonRole, listCharts);
router.get("/chartreal", isAuthenticatedChart, commonRole, realTimeChart);
router.get("/chart", isAuthenticatedChart, commonRole, readInflux);
module.exports = router;
