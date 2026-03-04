import type express from "express";

import { providerDataService } from "../../services/index.js";
import { parseYear, toCsv } from "../../utils.js";
import { parseProviderFilters } from "../providerFilters.js";

export async function getProviderAnnualData(req: express.Request, res: express.Response) {
  const filters = parseProviderFilters(req);
  const yearNum = parseYear(filters.year);
  
  if (!yearNum) return res.status(400).json({ error: "Invalid year parameter" });

  try {
    const data = await providerDataService.getYearlyData(filters);
    res.json(data);
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}

export async function exportProviderDataYearly(req: express.Request, res: express.Response) {
  const filters = parseProviderFilters(req);
  const yearNum = parseYear(filters.year);

  if (!yearNum) return res.status(400).json({ error: "Invalid year parameter" });

  try {
    const data = await providerDataService.getYearlyData(filters);

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
    res.setHeader("Content-Disposition", `attachment; filename="providers_${yearNum}.csv"`);
    res.send(csv);
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}
