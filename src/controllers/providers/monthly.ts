import type express from "express";

import { providerDataService } from "../../services/index.js";
import { toCsv } from "../../utils.js";
import { parseProviderFilters } from "../providerFilters.js";

export async function getProviderMonthData(req: express.Request, res: express.Response) {
  try {
    const filters = parseProviderFilters(req);
    const data = await providerDataService.getMonthlyData(filters);
    res.json(data);
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}

export async function exportProviderDataMonthly(req: express.Request, res: express.Response) {
  try {
    const filters = parseProviderFilters(req);
    const data = await providerDataService.getMonthlyData(filters);

    const csvRows = data.map(item => ({
      provider_licensing_id: item.providerLicensingId,
      provider_name: item.providerName,
      total_billed_over_capacity: item.childrenBilledOverCapacity,
      total_placed_over_capacity: item.childrenPlacedOverCapacity,
      total_distance_traveled: item.distanceTraveled,
      total_same_address: item.providersWithSameAddress,
      overall_risk_score: item.overallRiskScore,
    }));

    const csv = toCsv(csvRows, Object.keys(csvRows[0] || {}) as any);
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="providers_${filters.month}.csv"`);
    res.send(csv);
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}
