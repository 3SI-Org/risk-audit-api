import type express from "express";

import { queryData } from "../config/databricks.js";
import { buildProviderMonthlyFacetQuery, checkedFilter } from "../queryBuilders/providers/monthly.js";
import { buildProviderYearlyFacetQuery } from "../queryBuilders/providers/yearly.js";

export type ProviderFilters = {
  year: string;
  month?: string;
  offset?: string;
  flagStatus?: boolean | null;
  cities?: string[];
  // cityName for iLike searches
  cityName?: string;
  licenseCapacity?: string;
};

function parseDate(date: string): { year: string; month?: string } {
  const parts = date.split("-");
  return {
    year: parts[0],
    month: parts.length > 1 ? date : undefined, // "2024-01" or undefined
  };
}

export function parseProviderFilters(req: express.Request): ProviderFilters {
  const { year, month } = req.params.date
    ? parseDate(req.params.date)
    : { year: "2024", month: undefined };

  const isFlagged = req.query.flagStatus === "true";
  const isUnflagged = req.query.flagStatus === "false";

  return {
    year,
    month,
    offset: String(req.query.offset || "0"),
    flagStatus: checkedFilter({ flagged: isFlagged, unflagged: isUnflagged }),
    cities: Array.isArray(req.query.cities)
      ? req.query.cities.map(String)
      : req.query.cities ? [String(req.query.cities)] : [],
    cityName: (req.query?.cityName as string) || "",
    licenseCapacity: req.query?.licensedCapacity as string | undefined,
  };
}

function buildFacetQuery(
  target: string,
  date: string,
  params: Record<string, any>,
  search?: string,
  limit?: boolean,
) {
  const { year, month } = parseDate(date);
  return month
    ? buildProviderMonthlyFacetQuery(target, { ...params, month }, search, limit)
    : buildProviderYearlyFacetQuery(target, { ...params, year: year! }, search, limit);
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
