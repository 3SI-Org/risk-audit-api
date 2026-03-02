import { Router } from "express";

import { billedOverCapacityById, distanceTraveledById, overallScoreById, placedOverCapacityById, sameAddressById } from "../controllers/providerScenario.js";
import { authenticateJWT } from "../middlewares.js";

const router = Router();


router.route("/overall/:providerId").get(authenticateJWT, overallScoreById);

router.route("/placed/:providerId").get(authenticateJWT, placedOverCapacityById);

router.route("/billed/:providerId").get(authenticateJWT, billedOverCapacityById);

router.route("/address/:providerId").get(authenticateJWT, sameAddressById);

router.route("/distance/:providerId").get(authenticateJWT,distanceTraveledById );

export default router;
