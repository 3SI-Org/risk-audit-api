import { SQL } from "sql-template-strings";

import type { ProviderFilters } from "../../controllers/providerFilters.js";

import { parseOffsetParam } from "./monthly.js";

function parseRange(value?: string | undefined): { min: number | null; max: number | null } {
  if (!value)
    return { min: null, max: null };
  const [minStr, maxStr] = value.split(",");
  return {
    min: minStr?.trim() ? Number(minStr) : null,
    max: maxStr?.trim() ? Number(maxStr) : null,
  };
}

type FilterConfig = {
  name: string;
  selectExpr: string; // what to SELECT DISTINCT for facet
  nullFilter: string;
  searchable: boolean; // exclude nulls from facet results
  facetType?: "distinct" | "range"; // default "distinct"
  applyWhere: (
    sqlQuery: any,
    params: ProviderFilters
  ) => void;
  applyParams: (
    params: ProviderFilters,
    named: Record<string, any>
  ) => void;
  isActive: (params: ProviderFilters) => boolean;
};

const FILTERS: FilterConfig[] = [
  {
    name: "flagStatus",
    selectExpr: "pi.is_flagged",
    nullFilter: "AND pi.is_flagged IS NOT NULL",
    searchable: false,
    applyWhere: (sql, params) => {
      if (params.flagStatus !== null && params.flagStatus !== false) {
        sql.append(SQL` AND pi.is_flagged = :flagStatus`);
      }
      if (params.flagStatus === false) {
        sql.append(SQL` AND (pi.is_flagged IS NULL OR pi.is_flagged = :flagStatus)`);
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
    selectExpr: "a.city",
    nullFilter: "AND a.city IS NOT NULL",
    searchable: true,
    applyWhere: (sql, params) => {
      if (params.cities && params?.cities?.length > 0) {
        sql.append(SQL` AND ARRAY_CONTAINS(TRANSFORM(SPLIT(:cities, ','), s -> TRIM(s)), a.city)`);
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
    selectExpr: "c.capacity_licensed",
    nullFilter: "AND c.capacity_licensed IS NOT NULL",
    searchable: false,
    facetType: "range",
    applyWhere: (sql, params) => {
      const { min, max } = parseRange(params.licenseCapacity);
      if (min !== null) {
        sql.append(SQL` AND c.capacity_licensed >= :capacityMin`);
      }
      if (max !== null) {
        sql.append(SQL` AND c.capacity_licensed <= :capacityMax`);
      }
    },
    applyParams: (params, named) => {
      const { min, max } = parseRange(params.licenseCapacity);
      if (min !== null)
        named.capacityMin = min;
      if (max !== null)
        named.capacityMax = max;
    },
    isActive: (params) => {
      const { min, max } = parseRange(params.licenseCapacity);
      return min !== null || max !== null;
    },
  },
  // ---- add new filters here ----
  // {
  //   name: "zipCodes",
  //   selectExpr: "a.zip",
  //   nullFilter: "AND a.zip IS NOT NULL",
  //   applyWhere: (sql, params) => { ... },
  //   applyParams: (params, named) => { ... },
  //   isActive: (params) => params.zipCodes.length > 0,
  // },
];

function appendBaseCTE(sqlQuery: any) {
  sqlQuery.append(SQL`
    WITH combined AS (
      SELECT
        rp.provider_licensing_id,
        rp.provider_address_uid,
        rp.capacity_licensed,
        rp.provider_facility_type,
        rp.provider_status,
        COALESCE(b.total_billed_over_capacity, 0) AS total_billed_over_capacity,
        COALESCE(p.total_placed_over_capacity, 0) AS total_placed_over_capacity,
        COALESCE(p.total_child_placements, 0) AS total_child_placements,
        COALESCE(d.total_distance_traveled, 0) AS total_distance_traveled,
        COALESCE(s.total_same_address, 0) AS total_same_address,
        COALESCE(b.total_billed_over_capacity, 0) +
        COALESCE(p.total_placed_over_capacity, 0) +
        COALESCE(d.total_distance_traveled, 0) +
        COALESCE(s.total_same_address, 0) AS overall_risk_score
      FROM cusp_audit.demo.risk_providers rp
      LEFT JOIN (
        SELECT provider_licensing_id,
          SUM(CASE WHEN billed_over_capacity_flag THEN 1 ELSE 0 END) AS total_billed_over_capacity
        FROM cusp_audit.demo.monthly_billed_over_capacity
        WHERE YEAR(CAST(StartOfMonth AS DATE)) = :year
        GROUP BY provider_licensing_id
      ) b ON rp.provider_licensing_id = b.provider_licensing_id
      LEFT JOIN (
        SELECT provider_licensing_id,
          SUM(CASE WHEN placed_over_capacity_flag THEN 1 ELSE 0 END) AS total_placed_over_capacity,
          SUM(child_placements) AS total_child_placements
        FROM cusp_audit.demo.monthly_placed_over_capacity
        WHERE YEAR(CAST(StartOfMonth AS DATE)) = :year
        GROUP BY provider_licensing_id
      ) p ON rp.provider_licensing_id = p.provider_licensing_id
      LEFT JOIN (
        SELECT provider_licensing_id,
          SUM(CASE WHEN distance_traveled_flag THEN 1 ELSE 0 END) AS total_distance_traveled
        FROM cusp_audit.demo.monthly_distance_traveled
        WHERE YEAR(CAST(StartOfMonth AS DATE)) = :year
        GROUP BY provider_licensing_id
      ) d ON rp.provider_licensing_id = d.provider_licensing_id
      LEFT JOIN (
        SELECT provider_licensing_id,
          SUM(CASE WHEN same_address_flag THEN 1 ELSE 0 END) AS total_same_address
        FROM cusp_audit.demo.monthly_providers_with_same_address
        WHERE YEAR(CAST(StartOfMonth AS DATE)) = :year
        GROUP BY provider_licensing_id
      ) s ON rp.provider_licensing_id = s.provider_licensing_id
    )`);
}

function appendJoins(sqlQuery: any) {
  sqlQuery.append(SQL`
    FROM combined c
    LEFT JOIN cusp_audit.demo.provider_attributes pa ON c.provider_licensing_id = pa.provider_licensing_id
    LEFT JOIN cusp_audit.demo.provider_insights pi ON c.provider_licensing_id = pi.provider_licensing_id
    LEFT JOIN cusp_audit.fake_data.addresses a ON c.provider_address_uid = a.provider_address_uid
    WHERE 1=1`);
}

export function buildProviderYearlyQuery(params: ProviderFilters) {
  const sqlQuery = SQL``;
  appendBaseCTE(sqlQuery);

  sqlQuery.append(SQL`
    SELECT
      c.provider_licensing_id,
      pa.provider_name,
      c.capacity_licensed,
      c.provider_facility_type,
      c.provider_status,
      c.total_child_placements,
      c.total_billed_over_capacity,
      c.total_placed_over_capacity,
      c.total_distance_traveled,
      c.total_same_address,
      c.overall_risk_score,
      pi.is_flagged,
      pi.comment,
      a.postal_address,
      a.city,
      a.zip`);

  appendJoins(sqlQuery);

  const namedParameters: Record<string, any> = { year: params.year };
  for (const filter of FILTERS) {
    filter.applyWhere(sqlQuery, params);
    filter.applyParams(params, namedParameters);
  }

  sqlQuery.append(SQL` ORDER BY c.overall_risk_score DESC, c.provider_licensing_id`);
  sqlQuery.append(SQL` LIMIT 200 OFFSET :offset`);
  namedParameters.offset = parseOffsetParam(params.offset);
  // console.info(sqlQuery.text)
  return { text: sqlQuery.text, namedParameters };
}

export function buildProviderYearlyFacetQuery(
  target: string,
  params: Partial<Omit<ProviderFilters, "offset">> & { year: string },
  search?: string,
  limit?: boolean,
) {
  const filter = FILTERS.find(f => f.name === target);
  if (!filter)
    throw new Error(`Unknown filter: ${target}`);

  const sqlQuery = SQL``;
  appendBaseCTE(sqlQuery);

  // Select based on facet type
  if (filter.facetType === "range") {
    sqlQuery.append(` SELECT MIN(${filter.selectExpr}) AS min_value, MAX(${filter.selectExpr}) AS max_value`);
  }
  else {
    sqlQuery.append(` SELECT DISTINCT ${filter.selectExpr} AS option_value`);
  }

  appendJoins(sqlQuery);

  const namedParameters: Record<string, any> = { year: params.year };
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

  // Only order/limit for distinct facets
  if (filter.facetType !== "range") {
    sqlQuery.append(` ORDER BY option_value`);
    if (limit) {
      sqlQuery.append(SQL` LIMIT 100`);
    }
  }

  return { text: sqlQuery.text, namedParameters };
}
