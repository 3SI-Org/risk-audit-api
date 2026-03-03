// import SQL from "sql-template-strings";

// type BuildProviderMonthlyQueryParams = {
//   flagStatus: boolean | null;
//   month: string;
//   offset: string;
//   cities: string[];
// };

// export function buildProviderMonthlyQuery({ month, offset, flagStatus, cities }: BuildProviderMonthlyQueryParams) {

//   const query= SQL`
//   SELECT
//     dates.provider_licensing_id,
//     rp.provider_name,
//     pi.is_flagged,
//     pi.comment,
//     a.postal_address,
//     a.city,
//     a.zip,
//     dates.startOfMonth,
//     dates.over_billed_capacity,
//     dates.over_placement_capacity,
//     dates.same_address_flag,
//     dates.distance_traveled_flag,
//     (
//         coalesce(dates.over_billed_capacity::int, 0) +
//         coalesce(dates.over_placement_capacity::int, 0) +
//         coalesce(dates.same_address_flag::int, 0) +
//         coalesce(dates.distance_traveled_flag::int, 0)
//     ) as total
//   FROM (
//       SELECT
//         StartOfMonth,
//         over_billed_capacity,
//         over_placement_capacity,
//         same_address_flag,
//         distance_traveled_flag,
//         provider_licensing_id
//       FROM cusp_audit.demo.risk_scores
//       WHERE StartOfMonth = :month
//   ) as dates
//   JOIN cusp_audit.demo.risk_providers rp ON rp.provider_licensing_id = dates.provider_licensing_id
//   LEFT JOIN cusp_audit.demo.provider_insights pi ON rp.provider_licensing_id = pi.provider_licensing_id
//   LEFT JOIN cusp_audit.fake_data.addresses a ON rp.provider_address_uid = a.provider_address_uid
//   WHERE 1=1`;

//   if (flagStatus !== null && flagStatus !== false) {
//     query.append(SQL` AND pi.is_flagged = :flagStatus`);
//   }
//   // get records that have not been flagged prior, then unflagged
//   if (flagStatus === false) {
//     query.append(SQL` AND (pi.is_flagged IS NULL OR pi.is_flagged = :flagStatus)`);
//   }

//   if (cities.length > 0) {
//     query.append(SQL` AND ARRAY_CONTAINS(TRANSFORM(SPLIT(:cities, ','), s -> TRIM(s)), a.city)`)
//   }

//   // ---- append filter to the query above this line ----
//   query.append(SQL` ORDER BY total DESC, dates.provider_licensing_id`);
//   // offset is set to change by 200 each time from FE
//   query.append(SQL` limit 200 offset :offset`);

//   const namedParameters = {
//     month: parseMonthParam(month),
//     offset: parseOffsetParam(offset),
//     ...(flagStatus !== null ? { flagStatus } : {}),
//     ...(cities.length > 0 ? { cities: cities.join(",") } : {})
//   };

//   return { text: query.text, namedParameters };
// }

import { SQL } from "sql-template-strings";

import type { ProviderFilters } from "../../controllers/providerFilters.js";

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

type FilterConfig = {
  name: string;
  selectExpr: string;
  nullFilter: string;
  searchable: boolean;
  facetType?: "distinct" | "range";
  applyWhere: (sqlQuery: any, params: Partial<ProviderFilters>) => void;
  applyParams: (params: Partial<ProviderFilters>, named: Record<string, any>) => void;
  isActive: (params: Partial<ProviderFilters>) => boolean;
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
      if (params.cities && params.cities.length > 0) {
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
    selectExpr: "rp.capacity_licensed",
    nullFilter: "AND rp.capacity_licensed IS NOT NULL",
    searchable: false,
    facetType: "range",
    applyWhere: (sql, params) => {
      const { min, max } = parseRange(params.licenseCapacity);
      if (min !== null) {
        sql.append(SQL` AND rp.capacity_licensed >= :capacityMin`);
      }
      if (max !== null) {
        sql.append(SQL` AND rp.capacity_licensed <= :capacityMax`);
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
];

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
  appendBaseQuery(sqlQuery);
  appendJoins(sqlQuery);

  const namedParameters: Record<string, any> = { month: parseMonthParam(params.month!) };
  for (const filter of FILTERS) {
    filter.applyWhere(sqlQuery, params);
    filter.applyParams(params, namedParameters);
  }

  sqlQuery.append(SQL` ORDER BY total DESC, dates.provider_licensing_id`);
  sqlQuery.append(SQL` LIMIT 200 OFFSET :offset`);
  namedParameters.offset = parseOffsetParam(params.offset);

  return { text: sqlQuery.text, namedParameters };
}

export function buildProviderMonthlyFacetQuery(
  target: string,
  params: Partial<Omit<ProviderFilters, "offset">> & { month: string },
  search?: string,
  limit?: boolean,
) {
  const filter = FILTERS.find(f => f.name === target);
  if (!filter)
    throw new Error(`Unknown filter: ${target}`);

  const sqlQuery = SQL``;

  if (filter.facetType === "range") {
    sqlQuery.append(` SELECT MIN(${filter.selectExpr}) AS min_value, MAX(${filter.selectExpr}) AS max_value`);
  }
  else {
    sqlQuery.append(` SELECT DISTINCT ${filter.selectExpr} AS option_value`);
  }

  appendJoins(sqlQuery);

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
