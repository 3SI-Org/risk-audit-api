import type express from "express";

import { queryData } from "../config/databricks.js";
import { buildProviderDetailsQuery } from "../queryBuilders/providerDetails.js";
import { buildProviderMonthlyQuery } from "../queryBuilders/providerMonthly.js";
import { buildFlaggedCountQuery, buildHighestRiskScoreQuery, buildHighRiskCountQuery, buildProviderCountQuery } from "../queryBuilders/providers/overview.js";
import { buildProviderYearlyFacetQuery, buildProviderYearlyQuery } from "../queryBuilders/providerYearly.js";
import { parseProviderFilters } from "./parseProviderFilters.js";

export type MonthlyProviderData = {
  provider_licensing_id: string;
  provider_name: string;
  StartOfMonth: string; // ISO DateString
  over_billed_capacity: boolean;
  over_placement_capacity: number;
  same_address_flag: number;
  distance_traveled_flag: number;
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

export type ProviderDetailsData = {
  provider_licensing_id: string;
  provider_name: string;
  postal_address: string;
  city: string;
  zip: string;
  provider_status: string;
  provider_type: string;
  provider_email: string;
  provider_phone: string;
};

export type UiProviderDetailsData = {
  providerLicensingId: string;
  providerName: string;
  postalAddress: string;
  city: string;
  zip: string;
  providerStatus: string;
  providerType: string;
  providerEmail: string;
  providerPhone: string;
};

export async function exportProviderDataMonthly(req: express.Request, res: express.Response) {
  const filters = parseProviderFilters(req);
  // TODO:
  const { text, namedParameters } = buildProviderMonthlyQuery({ offset: filters.offset, month: filters.month!, flagStatus: filters.flagStatus, cities: filters.cities });

  try {
    // const rawData: MonthlyProviderData[] = await queryData(text, namedParameters);
    const rawData = await queryData(text, namedParameters);
    const result = rawData.map((item: any) => {
      return {
        // TODO - fix data types
        provider_licensing_id: item.provider_licensing_id,
        provider_name: item.provider_name,
        total_billed_over_capacity: item.over_billed_capacity ? "Yes" : "--",
        total_placed_over_capacity: item.over_placement_capacity ? "Yes" : "--",
        total_distance_traveled: item.distance_traveled_flag ? "Yes" : "--",
        total_same_address: item.same_address_flag ? "Yes" : "--",
        overall_risk_score: item.total || 0,
        // flagged: item?.is_flagged || false,
        // comment: item?.comment || "",
        // postalAddress: item.postal_address || "--",
        // city: item.city || "--",
        // zip: item.zip || "--",
      };
    });
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
    res.setHeader("Content-Disposition", `attachment; filename="providers_${filters.month}.csv"`);

    res.send(csv);
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}

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

export async function getProviderCount(req: express.Request, res: express.Response) {
  const { text, namedParameters } = buildProviderCountQuery(req);

  try {
    const data = await queryData(text, namedParameters);
    res.json(data[0]);
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}

export async function getHighestRiskScore(req: express.Request, res: express.Response) {
  const { text, namedParameters } = buildHighestRiskScoreQuery(req)

  try {
    const data = await queryData(text, namedParameters );
    // console.log("highRiskScore data ====", data);
    res.json(data);
  }
  catch (err: any) {
    console.log("err =======", err);
    res.status(500).json({ error: err.message });
  }
}

export async function getProvidersWithHighRiskCount(req: express.Request, res: express.Response) {
  const yearNum = Number.parseInt(req.params.date, 10);
  if (Number.isNaN(yearNum) || yearNum < 1980 || yearNum > 2100) {
    return res.status(400).json({ error: "Invalid year parameter" });
  }
  const { text, namedParameters } = buildHighRiskCountQuery(req)

  try {
    const data = await queryData(text, namedParameters);
    res.json(data[0]);
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}

export async function getFlaggedCount(req: express.Request, res: express.Response) {
  // const yearNum = Number.parseInt(req.params.year, 10);

  const {text} = buildFlaggedCountQuery();

  try {
    const data = await queryData(text);
    res.json(data[0]);
  }
  catch (err: any) {
    // console.log("err =======", err);
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

export async function getLicenseCapacity(req: express.Request, res: express.Response) {
  const filters = parseProviderFilters(req);

  const sql = filters?.month
    ? { text: "", namedParameters: { month: filters.month } }
    : buildProviderYearlyFacetQuery("licenseCapacity", { year: filters.year, flagStatus: filters.flagStatus, cities: filters.cities });

  try {
    const rawData = await queryData(sql.text, sql.namedParameters) as { min_value: number | null; max_value: number | null }[];

    res.json(rawData[0] ?? { min_value: null, max_value: null });
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}

export async function getProviderMonthData(req: express.Request, res: express.Response) {
  const filters = parseProviderFilters(req);
  const { text, namedParameters } = buildProviderMonthlyQuery({ offset: filters.offset, month: filters.month!, flagStatus: filters.flagStatus, cities: filters.cities });

  try {
    const rawData = await queryData(text, namedParameters) as MonthlyProviderData[];
    const result: UiMonthlyProviderData[] = rawData.map((item) => {
      // TODO Taylor / Justin - update types
      return {
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
      };
    });
    res.json(result);
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}

// TODO: add Index on city then add SORT BY then remove JS sort here
export async function getProviderCities(req: express.Request, res: express.Response) {
  const filters = parseProviderFilters(req);

  const sql = filters.month
    ? { text: "", namedParameters: { month: filters.month } }
    : buildProviderYearlyFacetQuery("cities", { year: filters.year!, flagStatus: filters.flagStatus, cities: [], licenseCapacity: filters.licenseCapacity }, filters.cityName, true);

  try {
    const rawData = (await queryData(sql.text, sql.namedParameters)) as {
      option_value: any;
      city: string;
    }[];
    const parsed = rawData.map(item => item.option_value);

    res.json(parsed);
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}

export async function getProviderDetails(req: express.Request, res: express.Response) {
  const provider_licensing_id = req.params.providerId;
  const { text, namedParameters } = buildProviderDetailsQuery({ provider_licensing_id });

  try {
    const rawData = await queryData(text, namedParameters) as ProviderDetailsData[];
    const result: UiProviderDetailsData[] = rawData.map((item) => {
      return {
        providerLicensingId: item.provider_licensing_id,
        providerName: item.provider_name,
        postalAddress: item.postal_address || "--",
        city: item.city || "--",
        zip: item.zip || "--",
        providerPhone: item.provider_phone || "--",
        providerEmail: item.provider_email || "--",
        providerStatus: item.provider_status || "--",
        providerType: item.provider_type || "--",
      };
    });
    res.json(result[0]);
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}
