import { Router } from "express";

import { getLicenseCapacity, getProviderCities } from "../controllers/providerFilters.js";
import { getProviderDataInsights, updateProviderDataInsights } from "../controllers/providerInsights.js";
import { getProviderDetails } from "../controllers/providers/details.js";
import { exportProviderDataMonthly, getProviderMonthData } from "../controllers/providers/monthly.js";
import { getFlaggedCount, getHighestRiskScore, getProviderCount, getProvidersWithHighRiskCount } from "../controllers/providers/overview.js";
import { exportProviderDataYearly, getProviderAnnualData } from "../controllers/providers/yearly.js";
import { authenticateJWT } from "../middlewares.js";

const router = Router();

router.route("/cities/:date").get(authenticateJWT, getProviderCities);
router.route("/license-capacity/:date").get(authenticateJWT, getLicenseCapacity);

router.route("/export/year/:date")
  .get(authenticateJWT, exportProviderDataYearly);

router.route("/export/month/:date")
  .get(authenticateJWT, exportProviderDataMonthly);

router.route("/providerCount/:date")
  .get(authenticateJWT, getProviderCount);

router.route("/flaggedCount/:date")
  .get(authenticateJWT, getFlaggedCount);

router.route("/highRiskScore/:date")
  .get(authenticateJWT, getHighestRiskScore);

router.route("/highRiskScoreCount/:date")
  .get(authenticateJWT, getProvidersWithHighRiskCount);

router.route("/annual/:date")
  .get(authenticateJWT, getProviderAnnualData);

router.route("/insights/:providerId")
  .put(authenticateJWT, updateProviderDataInsights)
  .get(authenticateJWT, getProviderDataInsights);

router.route("/month/:date")
  .get(
    authenticateJWT,
    getProviderMonthData,
  );
// must be last!!
router.route("/:providerId")
  .get(
    authenticateJWT,
    getProviderDetails,
  );

export default router;
