import type express from "express";

import type { ProviderFilters } from "../types/provider.js";

import { queryData } from "../config/databricks.js";
import { buildProviderMonthlyFacetQuery, checkedFilter } from "../queryBuilders/providers/monthly.js";
import { buildProviderYearlyFacetQuery } from "../queryBuilders/providers/yearly.js";

function parseDate(date: string): { year: string; month?: string } {
  const parts = date.split("-");
  return {
    year: parts[0],
    month: parts.length > 1 ? date : undefined, // "2024-01" or undefined
  };
}

// Default to values that will not limit results if values are missing
export function parseProviderFilters(req: express.Request): ProviderFilters {
  const { year, month } = parseDate(req.params.date);

  return {
    year: year ?? "",
    month: month ?? "",
    offset: String(req.query.offset || "0"),
    flagStatus: checkedFilter({
      flagged: req.query.flagStatus === "true",
      unflagged: req.query.flagStatus === "false",
    }),
    cities: (Array.isArray(req.query.cities)
      ? req.query.cities.map(String)
      : req.query.cities ? [String(req.query.cities)] : []
    ).filter(c => c.trim() !== ""),
    licenseCapacity: (req.query?.licensedCapacity as string) || "",
    overallRiskScore: (req.query?.overallRiskScore as string) || "",
  };
}

function buildFacetQuery(
  target: string,
  date: string,
  params: ProviderFilters,
  search?: string,
  limit?: boolean,
) {
  const { month } = parseDate(date);
  return month
    ? buildProviderMonthlyFacetQuery(target, { ...params }, search, limit)
    : buildProviderYearlyFacetQuery(target, { ...params }, search, limit);
}

export async function getProviderCities(req: express.Request, res: express.Response) {
  const filters = parseProviderFilters(req);
  const cityName = req.query?.cityName as string || "";
  const sql = buildFacetQuery("cities", req.params.date, filters, cityName, true);

  try {
    const rawData = await queryData(sql.text, sql.namedParameters) as any[];
    res.json(rawData.map(item => item.option_value));
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}

export async function getLicenseCapacity(req: express.Request, res: express.Response) {
  const filters = parseProviderFilters(req);
  const sql = buildFacetQuery("licenseCapacity", req.params.date, filters);

  try {
    const rawData = await queryData(sql.text, sql.namedParameters) as any[];
    res.json(rawData[0] ?? { min_value: null, max_value: null });
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}

export async function getOverallRiskScore(req: express.Request, res: express.Response) {
  const filters = parseProviderFilters(req);
  const sql = buildFacetQuery("overallRiskScore", req.params.date, filters);

  try {
    const rawData = await queryData(sql.text, sql.namedParameters) as any[];
    res.json(rawData[0] ?? { min_value: null, max_value: null });
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}

