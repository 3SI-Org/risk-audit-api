import SQL from "sql-template-strings";

type BuildProviderMonthlyQueryParams = {
  flagStatus: boolean | null;
  month: string;
  offset: string;
  cities: string[];
};

export function checkedFilter(filterOptions: { flagged: boolean, unflagged: boolean }): boolean | null {
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

export function parseOffsetParam(offsetParam: string): number {
  if (typeof offsetParam === "string") {
    const parsed = Number(offsetParam);
    return Number.isNaN(parsed) || parsed < 0 ? 0 : parsed;
  }
  return 0;
}

export function buildProviderMonthlyQuery({ month, offset, flagStatus, cities }: BuildProviderMonthlyQueryParams) {

  const query= SQL`
  SELECT
    dates.provider_licensing_id,
    rp.provider_name,
    pi.is_flagged,
    pi.comment,
    a.postal_address,
    a.city,
    a.zip,
    dates.startOfMonth,
    dates.over_billed_capacity,
    dates.over_placement_capacity,
    dates.same_address_flag,
    dates.distance_traveled_flag,
    (
        coalesce(dates.over_billed_capacity::int, 0) +
        coalesce(dates.over_placement_capacity::int, 0) +
        coalesce(dates.same_address_flag::int, 0) +
        coalesce(dates.distance_traveled_flag::int, 0)
    ) as total
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
  ) as dates
  JOIN cusp_audit.demo.risk_providers rp ON rp.provider_licensing_id = dates.provider_licensing_id
  LEFT JOIN cusp_audit.demo.provider_insights pi ON rp.provider_licensing_id = pi.provider_licensing_id
  LEFT JOIN cusp_audit.fake_data.addresses a ON rp.provider_address_uid = a.provider_address_uid
  WHERE 1=1`;

  if (flagStatus !== null && flagStatus !== false) {
    query.append(SQL` AND pi.is_flagged = :flagStatus`);
  }
  // get records that have not been flagged prior, then unflagged
  if (flagStatus === false) {
    query.append(SQL` AND (pi.is_flagged IS NULL OR pi.is_flagged = :flagStatus)`);
  }

  if (cities.length > 0) {
    query.append(SQL` AND ARRAY_CONTAINS(TRANSFORM(SPLIT(:cities, ','), s -> TRIM(s)), a.city)`)
  }

  // ---- append filter to the query above this line ----
  query.append(SQL` ORDER BY total DESC, dates.provider_licensing_id`);
  // offset is set to change by 200 each time from FE
  query.append(SQL` limit 200 offset :offset`);

  const namedParameters = {
    month: parseMonthParam(month),
    offset: parseOffsetParam(offset),
    ...(flagStatus !== null ? { flagStatus } : {}),
    ...(cities.length > 0 ? { cities: cities.join(",") } : {})
  };

  return { text: query.text, namedParameters };
}
