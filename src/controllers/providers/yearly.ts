import type express from "express";

import { queryData } from "../../config/databricks.js";
import { buildProviderYearlyQuery } from "../../queryBuilders/providers/yearly.js";
import { parseProviderFilters } from "../providerFilters.js";

export type AnnualProviderData = {
  capacity_licensed: string;
  provider_facility_type: string;
  provider_status: string;
  provider_licensing_id: string;
  provider_name: string;
  StartOfMonth: string; // ISO DateString
  total_billed_over_capacity: number;
  total_placed_over_capacity: number;
  total_same_address: number;
  total_distance_traveled: number;
  overall_risk_score: number;
  is_flagged: boolean;
  comment: string;
  postal_address: string;
  city: string;
  zip: string;
};

export type UiAnnualProviderData = {
  providerLicensingId: string;
  providerName: string;
  childrenBilledOverCapacity: number;
  childrenPlacedOverCapacity: number;
  distanceTraveled: number;
  overallRiskScore: number;
  providersWithSameAddress: number;
  flagged: boolean;
  comment: string;
  postalAddress: string;
  city: string;
  zip: string;
};

export async function exportProviderDataYearly(req: express.Request, res: express.Response) {
  const yearNum = Number.parseInt(req.params.date, 10);
  if (Number.isNaN(yearNum) || yearNum < 1980 || yearNum > 2100) {
    return res.status(400).json({ error: "Invalid year parameter" });
  }

  const filters = parseProviderFilters(req);

  const { text, namedParameters } = buildProviderYearlyQuery({ offset: filters.offset, year: String(yearNum), flagStatus: filters.flagStatus, cities: filters.cities, licenseCapacity: filters.licenseCapacity });
  try {
    const rawData = await queryData(text, namedParameters);
    const result: Partial<AnnualProviderData>[] = rawData.map((item: any) => {
      // TODO - fix data types
      return {
        provider_licensing_id: item.provider_licensing_id,
        provider_name: item.provider_name ? item.provider_name : "--",
        total_billed_over_capacity: item.total_billed_over_capacity || 0,
        total_placed_over_capacity: item.total_placed_over_capacity || 0,
        total_distance_traveled: item.total_distance_traveled || 0,
        total_same_address: item.total_same_address || 0,
        overall_risk_score: item.overall_risk_score || 0,
        // flagged: item?.is_flagged || false,
        // comment: item?.comment || "",
        // postalAddress: item.postal_address || "--",
        // city: item.city || "--",
        // zip: item.zip || "--",
      };
    });

    // console.log("REESULT", result)
    // res.json(result);

    // Build CSV manually
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
      headers.join(","), // header row
      ...result.map(row =>
        headers.map(h => escape((row as any)[h])).join(","),
      ),
    ];

    const csv = csvRows.join("\n");

    // Set headers for download
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="providers_${yearNum}.csv"`);

    res.send(csv);
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}

export async function getProviderAnnualData(req: express.Request, res: express.Response) {
  const filters = parseProviderFilters(req);

  const { text, namedParameters } = buildProviderYearlyQuery({
    ...filters,
  });

  if (Number.isNaN(Number(filters.year)) || Number(filters.year) < 1980 || Number(filters.year) > 2100) {
    return res.status(400).json({ error: "Invalid year parameter" });
  }

  try {
    const rawData = await queryData(text, namedParameters) as AnnualProviderData[];
    const result: UiAnnualProviderData[] = rawData.map((item) => {
      return {
        providerLicensingId: item?.provider_licensing_id,
        providerName: item?.provider_name ? item.provider_name : "--",
        childrenBilledOverCapacity: item?.total_billed_over_capacity || 0,
        childrenPlacedOverCapacity: item?.total_placed_over_capacity || 0,
        distanceTraveled: item?.total_distance_traveled || 0,
        providersWithSameAddress: item?.total_same_address || 0,
        overallRiskScore: item?.overall_risk_score || 0,
        flagged: item?.is_flagged || false,
        comment: item?.comment || "",
        postalAddress: item?.postal_address || "--",
        city: item?.city || "--",
        zip: item?.zip || "--",
        capacityLicensed: item?.capacity_licensed || "--",
        providerFacilityType: item?.provider_facility_type || "--",
        providerStatus: item?.provider_status || "--",
      };
    });

    res.json(result);
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}
