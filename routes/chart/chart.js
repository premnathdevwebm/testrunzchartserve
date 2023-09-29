const { Router } = require("express");

const { createChart, closeRunzChart, listCharts, readInflux } = require("../../controller");
const { isAuthenticatedChart, commonRole } = require("../../middleware");

const router = new Router();

router.post("/chart", /* isAuthenticatedChart, commonRole, */ createChart);
router.patch("/chart/:runzId", /* isAuthenticatedChart, commonRole, */ closeRunzChart);
router.get("/charts", isAuthenticatedChart, commonRole, listCharts);
router.get("/chart", isAuthenticatedChart, commonRole, readInflux);
module.exports = router;
