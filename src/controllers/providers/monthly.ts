import type express from "express";

import { queryData } from "../../config/databricks.js";
import { buildProviderMonthlyQuery } from "../../queryBuilders/providers/monthly.js";
import { parseProviderFilters } from "../providerFilters.js";

export type MonthlyProviderData = {
  provider_licensing_id: string;
  provider_name: string;
  StartOfMonth: string;
  over_billed_capacity: boolean;
  over_placement_capacity: boolean;
  same_address_flag: boolean;
  distance_traveled_flag: boolean;
  total: number;
  is_flagged: boolean;
  comment: string;
  postal_address: string;
  city: string;
  zip: string;
};

export type UiMonthlyProviderData = {
  providerLicensingId: string;
  providerName: string;
  overallRiskScore: number;
  childrenBilledOverCapacity: string;
  childrenPlacedOverCapacity: string;
  distanceTraveled: string;
  providersWithSameAddress: string;
  flagged?: boolean;
  comment?: string;
  startOfMonth?: string;
  postalAddress: string;
  city: string;
  zip: string;
};

export async function getProviderMonthData(req: express.Request, res: express.Response) {
  const filters = parseProviderFilters(req);
  const { text, namedParameters } = buildProviderMonthlyQuery(filters);

  try {
    const rawData = await queryData(text, namedParameters) as MonthlyProviderData[];
    const result: UiMonthlyProviderData[] = rawData.map(item => ({
      providerLicensingId: item.provider_licensing_id,
      startOfMonth: item.StartOfMonth,
      providerName: item.provider_name,
      childrenBilledOverCapacity: item.over_billed_capacity ? "Yes" : "--",
      childrenPlacedOverCapacity: item.over_placement_capacity ? "Yes" : "--",
      distanceTraveled: item.distance_traveled_flag ? "Yes" : "--",
      providersWithSameAddress: item.same_address_flag ? "Yes" : "--",
      overallRiskScore: item.total || 0,
      flagged: item?.is_flagged || false,
      comment: item?.comment || "",
      postalAddress: item.postal_address || "--",
      city: item.city || "--",
      zip: item.zip || "--",
    }));
    res.json(result);
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}

export async function exportProviderDataMonthly(req: express.Request, res: express.Response) {
  const filters = parseProviderFilters(req);
  const { text, namedParameters } = buildProviderMonthlyQuery(filters);

  try {
    const rawData = await queryData(text, namedParameters) as any[];
    const result = rawData.map(item => ({
      provider_licensing_id: item.provider_licensing_id,
      provider_name: item.provider_name,
      total_billed_over_capacity: item.over_billed_capacity ? "Yes" : "--",
      total_placed_over_capacity: item.over_placement_capacity ? "Yes" : "--",
      total_distance_traveled: item.distance_traveled_flag ? "Yes" : "--",
      total_same_address: item.same_address_flag ? "Yes" : "--",
      overall_risk_score: item.total || 0,
    }));

    const headers = [
      "provider_licensing_id",
      "provider_name",
      "total_billed_over_capacity",
      "total_placed_over_capacity",
      "total_distance_traveled",
      "total_same_address",
      "overall_risk_score",
    ];

    const escape = (val: any) => {
      if (val === null || val === undefined)
        return "";
      const str = String(val);
      return /[",\n]/.test(str) ? `"${str.replace(/"/g, "\"\"")}"` : str;
    };

    const csvRows = [
      headers.join(","),
      ...result.map(row =>
        headers.map(h => escape((row as any)[h])).join(","),
      ),
    ];

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="providers_${filters.month}.csv"`);
    res.send(csvRows.join("\n"));
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}
