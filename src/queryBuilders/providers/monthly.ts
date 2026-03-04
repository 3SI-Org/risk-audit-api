import { SQL } from "sql-template-strings";

import type { FilterConfig, ProviderFilters } from "../../types/provider.js";

export function checkedFilter(filterOptions: { flagged: boolean; unflagged: boolean }): boolean | null {
  if (filterOptions.flagged && !filterOptions.unflagged)
    return true;
  if (!filterOptions.flagged && filterOptions.unflagged)
    return false;
  return null;
}

export function parseMonthParam(monthParam: string) {
  // Expect format YYYY-MM
  const regex = /^\d{4}-(?:0[1-9]|1[0-2])$/;
  if (!regex.test(monthParam)) {
    throw new Error("Invalid month format, expected YYYY-MM");
  }
  return `${monthParam}-01`;
}

export function parseOffsetParam(offsetParam: string | undefined): number {
  if (typeof offsetParam === "string") {
    const parsed = Number(offsetParam);
    return Number.isNaN(parsed) || parsed < 0 ? 0 : parsed;
  }
  return 0;
}

function parseRange(value?: string | undefined): { min: number | null; max: number | null } {
  if (!value)
    return { min: null, max: null };
  const [minStr, maxStr] = value.split(",");
  return {
    min: minStr?.trim() ? Number(minStr) : null,
    max: maxStr?.trim() ? Number(maxStr) : null,
  };
}

const FILTERS: FilterConfig[] = [
  {
    name: "flagStatus",
    selectExpr: "base.is_flagged",
    nullFilter: "AND base.is_flagged IS NOT NULL",
    searchable: false,
    applyWhere: (sql, params) => {
      if (params.flagStatus !== null && params.flagStatus !== false) {
        sql.append(SQL` AND base.is_flagged = :flagStatus`);
      }
      if (params.flagStatus === false) {
        sql.append(SQL` AND (base.is_flagged IS NULL OR base.is_flagged = :flagStatus)`);
      }
    },
    applyParams: (params, named) => {
      if (params.flagStatus !== null)
        named.flagStatus = params.flagStatus;
    },
    isActive: params => params.flagStatus !== null,
  },
  {
    name: "cities",
    selectExpr: "base.city",
    nullFilter: "AND base.city IS NOT NULL",
    searchable: true,
    applyWhere: (sql, params) => {
      if (params.cities && params.cities.length > 0) {
        sql.append(SQL` AND ARRAY_CONTAINS(TRANSFORM(SPLIT(:cities, ','), s -> TRIM(s)), base.city)`);
      }
    },
    applyParams: (params, named) => {
      if (params.cities && params.cities.length > 0)
        named.cities = params.cities.join(",");
    },
    isActive: params => (params.cities && params.cities.length > 0) === true,
  },
  {
    name: "licenseCapacity",
    selectExpr: "base.capacity_licensed",
    nullFilter: "AND base.capacity_licensed IS NOT NULL",
    searchable: false,
    facetType: "range",
    applyWhere: (sql, params) => {
      const { min, max } = parseRange(params.licenseCapacity);
      if (min !== null) {
        sql.append(SQL` AND base.capacity_licensed >= :capacityMin`);
      }
      if (max !== null) {
        sql.append(SQL` AND base.capacity_licensed <= :capacityMax`);
      }
    },
    applyParams: (params, named) => {
      const { min, max } = parseRange(params.licenseCapacity);
      if (min !== null) named.capacityMin = min;
      if (max !== null) named.capacityMax = max;
    },
    isActive: (params) => {
      const { min, max } = parseRange(params.licenseCapacity);
      return min !== null || max !== null;
    },
  },
  {
    name: "overallRiskScore",
    selectExpr: "base.total",
    nullFilter: "AND base.total IS NOT NULL",
    searchable: false,
    facetType: "range",
    applyWhere: (sql, params) => {
      const { min, max } = parseRange(params.overallRiskScore);
      if (min !== null) {
        sql.append(SQL` AND base.total >= :riskScoreMin`);
      }
      if (max !== null) {
        sql.append(SQL` AND base.total <= :riskScoreMax`);
      }
    },
    applyParams: (params, named) => {
      const { min, max } = parseRange(params.overallRiskScore);
      if (min !== null) named.riskScoreMin = min;
      if (max !== null) named.riskScoreMax = max;
    },
    isActive: (params) => {
      const { min, max } = parseRange(params.overallRiskScore);
      return min !== null || max !== null;
    },
  },
];
// column alias can have filter refs; useful for total
function appendBaseCTE(sqlQuery: any) {
  sqlQuery.append(SQL`
    WITH base AS (
  `);
  appendBaseQuery(sqlQuery);
  appendJoins(sqlQuery);
  sqlQuery.append(SQL`
    )
  `);
}

function appendBaseQuery(sqlQuery: any) {
  sqlQuery.append(SQL`
    SELECT
      dates.provider_licensing_id,
      rp.provider_name,
      rp.capacity_licensed,
      rp.provider_facility_type,
      rp.provider_status,
      pi.is_flagged,
      pi.comment,
      a.postal_address,
      a.city,
      a.zip,
      dates.StartOfMonth,
      dates.over_billed_capacity,
      dates.over_placement_capacity,
      dates.same_address_flag,
      dates.distance_traveled_flag,
      (
        COALESCE(dates.over_billed_capacity::int, 0) +
        COALESCE(dates.over_placement_capacity::int, 0) +
        COALESCE(dates.same_address_flag::int, 0) +
        COALESCE(dates.distance_traveled_flag::int, 0)
      ) AS total`);
}

function appendJoins(sqlQuery: any) {
  sqlQuery.append(SQL`
    FROM (
      SELECT
        StartOfMonth,
        over_billed_capacity,
        over_placement_capacity,
        same_address_flag,
        distance_traveled_flag,
        provider_licensing_id
      FROM cusp_audit.demo.risk_scores
      WHERE StartOfMonth = :month
    ) AS dates
    JOIN cusp_audit.demo.risk_providers rp ON rp.provider_licensing_id = dates.provider_licensing_id
    LEFT JOIN cusp_audit.demo.provider_insights pi ON rp.provider_licensing_id = pi.provider_licensing_id
    LEFT JOIN cusp_audit.fake_data.addresses a ON rp.provider_address_uid = a.provider_address_uid
    WHERE 1=1`);
}

export function buildProviderMonthlyQuery(params: ProviderFilters) {
  const sqlQuery = SQL``;
  appendBaseCTE(sqlQuery);

  sqlQuery.append(SQL`
      SELECT * FROM base
      WHERE 1=1
    `)

  const namedParameters: Record<string, any> = { month: parseMonthParam(params.month) };
  for (const filter of FILTERS) {
    filter.applyWhere(sqlQuery, params);
    filter.applyParams(params, namedParameters);
  }

  sqlQuery.append(SQL` ORDER BY base.total DESC, base.provider_licensing_id`);
  sqlQuery.append(SQL` LIMIT 200 OFFSET :offset`);
  namedParameters.offset = parseOffsetParam(params.offset);
  return { text: sqlQuery.text, namedParameters };
}

export function buildProviderMonthlyFacetQuery(
  target: string,
  params: ProviderFilters,
  search?: string,
  limit?: boolean,
) {
  const filter = FILTERS.find(f => f.name === target);
  if (!filter)
    throw new Error(`Unknown filter: ${target}`);

  const sqlQuery = SQL``;
  appendBaseCTE(sqlQuery);

  if (filter.facetType === "range") {
    sqlQuery.append(` SELECT MIN(${filter.selectExpr}) AS min_value, MAX(${filter.selectExpr}) AS max_value FROM base WHERE 1=1`);
  } else {
    sqlQuery.append(` SELECT DISTINCT ${filter.selectExpr} AS option_value FROM base WHERE 1=1`);
  }

  const namedParameters: Record<string, any> = { month: parseMonthParam(params.month) };
  for (const f of FILTERS) {
    if (f.name === target)
      continue;
    f.applyWhere(sqlQuery, params);
    f.applyParams(params, namedParameters);
  }

  sqlQuery.append(` ${filter.nullFilter}`);

  if (search && filter.searchable) {
    sqlQuery.append(` AND ${filter.selectExpr} ILIKE :facetSearch`);
    namedParameters.facetSearch = `%${search}%`;
  }

  if (filter.facetType !== "range") {
    sqlQuery.append(` ORDER BY option_value`);
    if (limit) {
      sqlQuery.append(SQL` LIMIT 100`);
    }
  }

  return { text: sqlQuery.text, namedParameters };
}
