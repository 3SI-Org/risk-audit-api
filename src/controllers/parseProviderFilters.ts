import type express from "express";

import { checkedFilter } from "../queryBuilders/providerMonthly.js";

export type ProviderFilters = {
  year: string;
  month?: string;
  offset: string;
  flagStatus: boolean | null;
  cities: string[];
  cityName: string;
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
    : { year: '2024', month: undefined };

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
