import type express from "express";

export function buildProviderCountQuery(req: express.Request) {
  const yearNum = Number.parseInt(req.params.date, 10);

  return {
    text: `
      SELECT COUNT(DISTINCT provider_licensing_id) AS unique_provider_count
      FROM cusp_audit.demo.risk_scores
      WHERE YEAR(CAST(StartOfMonth AS DATE)) = :year
    `,
    namedParameters: { year: yearNum},
  };
}

export function buildFlaggedCountQuery() {
  return {
    text: `
      SELECT COUNT(DISTINCT provider_uid) AS flagged_provider_count
      FROM cusp_audit.demo.risk_providers a WHERE EXISTS (
        SELECT 1 FROM cusp_audit.demo.provider_insights b
        WHERE b.provider_licensing_id = a.provider_licensing_id AND b.is_flagged = 'true'
      )
    `,
  };
}

export function buildHighRiskCountQuery(req: express.Request) {
  const yearNum = Number.parseInt(req.params.date, 10);
  return {
    text: `
    WITH combined AS (
      SELECT
        COALESCE(b.provider_licensing_id, p.provider_licensing_id, d.provider_licensing_id, s.provider_licensing_id) AS provider_licensing_id,
        COALESCE(b.total_billed_over_capacity, 0) AS total_billed_over_capacity,
        COALESCE(p.total_placed_over_capacity, 0) AS total_placed_over_capacity,
        COALESCE(d.total_distance_traveled, 0) AS total_distance_traveled,
        COALESCE(s.total_same_address, 0) AS total_same_address,

        COALESCE(b.total_billed_over_capacity, 0) +
        COALESCE(p.total_placed_over_capacity, 0) +
        COALESCE(d.total_distance_traveled, 0) +
        COALESCE(s.total_same_address, 0) AS overall_risk_score
      FROM (
        SELECT provider_licensing_id,
          SUM(CASE WHEN billed_over_capacity_flag THEN 1 ELSE 0 END) AS total_billed_over_capacity    
        FROM cusp_audit.demo.monthly_billed_over_capacity
        WHERE YEAR(CAST(StartOfMonth AS DATE)) = :year
        GROUP BY provider_licensing_id
      ) b
      FULL OUTER JOIN (
        SELECT provider_licensing_id, 
          SUM(CASE WHEN placed_over_capacity_flag THEN 1 ELSE 0 END) AS total_placed_over_capacity    
        FROM cusp_audit.demo.monthly_placed_over_capacity
        WHERE YEAR(CAST(StartOfMonth AS DATE)) = :year
        GROUP BY provider_licensing_id
      ) p
        ON b.provider_licensing_id = p.provider_licensing_id
      FULL OUTER JOIN (
        SELECT provider_licensing_id, 
          SUM(CASE WHEN distance_traveled_flag THEN 1 ELSE 0 END) AS total_distance_traveled   
        FROM cusp_audit.demo.monthly_distance_traveled
        WHERE YEAR(CAST(StartOfMonth AS DATE)) = :year
        GROUP BY provider_licensing_id
      ) d
        ON COALESCE(b.provider_licensing_id, p.provider_licensing_id) = d.provider_licensing_id
      FULL OUTER JOIN (
        SELECT provider_licensing_id, 
          SUM(CASE WHEN same_address_flag THEN 1 ELSE 0 END) AS total_same_address      
        FROM cusp_audit.demo.monthly_providers_with_same_address
        WHERE YEAR(CAST(StartOfMonth AS DATE)) = :year
        GROUP BY provider_licensing_id
      ) s
        ON COALESCE(b.provider_licensing_id, p.provider_licensing_id, d.provider_licensing_id) = s.provider_licensing_id
    )
    SELECT COUNT(*) AS count_over_44
    FROM combined
    WHERE overall_risk_score > 44;
  `,
    namedParameters: { year: String(yearNum) },
  };
}

export function buildHighestRiskScoreQuery(req: express.Request) {
  const year1 = Number.parseInt(req.params.date, 10);
  return {
    text: `
      WITH combined AS (
        -- Year 1
        SELECT
          COALESCE(b.provider_licensing_id, p.provider_licensing_id, d.provider_licensing_id, s.provider_licensing_id) AS provider_licensing_id,
          COALESCE(b.total_billed_over_capacity, 0) AS total_billed_over_capacity,
          COALESCE(p.total_placed_over_capacity, 0) AS total_placed_over_capacity,
          COALESCE(d.total_distance_traveled, 0) AS total_distance_traveled,
          COALESCE(s.total_same_address, 0) AS total_same_address,
          :year1 AS year
        FROM (
          SELECT provider_licensing_id,
            SUM(CASE WHEN billed_over_capacity_flag THEN 1 ELSE 0 END) AS total_billed_over_capacity    
          FROM cusp_audit.demo.monthly_billed_over_capacity
          WHERE YEAR(CAST(StartOfMonth AS DATE)) = :year1
          GROUP BY provider_licensing_id
        ) b
        FULL OUTER JOIN (
          SELECT provider_licensing_id, 
            SUM(CASE WHEN placed_over_capacity_flag THEN 1 ELSE 0 END) AS total_placed_over_capacity    
          FROM cusp_audit.demo.monthly_placed_over_capacity
          WHERE YEAR(CAST(StartOfMonth AS DATE)) = :year1
          GROUP BY provider_licensing_id
        ) p
          ON b.provider_licensing_id = p.provider_licensing_id
        FULL OUTER JOIN (
          SELECT provider_licensing_id, 
            SUM(CASE WHEN distance_traveled_flag THEN 1 ELSE 0 END) AS total_distance_traveled   
          FROM cusp_audit.demo.monthly_distance_traveled
          WHERE YEAR(CAST(StartOfMonth AS DATE)) = :year1
          GROUP BY provider_licensing_id
        ) d
          ON COALESCE(b.provider_licensing_id, p.provider_licensing_id) = d.provider_licensing_id
        FULL OUTER JOIN (
          SELECT provider_licensing_id, 
            SUM(CASE WHEN same_address_flag THEN 1 ELSE 0 END) AS total_same_address      
          FROM cusp_audit.demo.monthly_providers_with_same_address
          WHERE YEAR(CAST(StartOfMonth AS DATE)) = :year1
          GROUP BY provider_licensing_id
        ) s
          ON COALESCE(b.provider_licensing_id, p.provider_licensing_id, d.provider_licensing_id) = s.provider_licensing_id

        UNION ALL

        -- Year 2
        SELECT
          COALESCE(b.provider_licensing_id, p.provider_licensing_id, d.provider_licensing_id, s.provider_licensing_id) AS provider_licensing_id,
          COALESCE(b.total_billed_over_capacity, 0) AS total_billed_over_capacity,
          COALESCE(p.total_placed_over_capacity, 0) AS total_placed_over_capacity,
          COALESCE(d.total_distance_traveled, 0) AS total_distance_traveled,
          COALESCE(s.total_same_address, 0) AS total_same_address,
          :year2 AS year
        FROM (
          SELECT provider_licensing_id,
            SUM(CASE WHEN billed_over_capacity_flag THEN 1 ELSE 0 END) AS total_billed_over_capacity    
          FROM cusp_audit.demo.monthly_billed_over_capacity
          WHERE YEAR(CAST(StartOfMonth AS DATE)) = :year2
          GROUP BY provider_licensing_id
        ) b
        FULL OUTER JOIN (
          SELECT provider_licensing_id, 
            SUM(CASE WHEN placed_over_capacity_flag THEN 1 ELSE 0 END) AS total_placed_over_capacity    
          FROM cusp_audit.demo.monthly_placed_over_capacity
          WHERE YEAR(CAST(StartOfMonth AS DATE)) = :year2
          GROUP BY provider_licensing_id
        ) p
          ON b.provider_licensing_id = p.provider_licensing_id
        FULL OUTER JOIN (
          SELECT provider_licensing_id, 
            SUM(CASE WHEN distance_traveled_flag THEN 1 ELSE 0 END) AS total_distance_traveled   
          FROM cusp_audit.demo.monthly_distance_traveled
          WHERE YEAR(CAST(StartOfMonth AS DATE)) = :year2
          GROUP BY provider_licensing_id
        ) d
          ON COALESCE(b.provider_licensing_id, p.provider_licensing_id) = d.provider_licensing_id
        FULL OUTER JOIN (
          SELECT provider_licensing_id, 
            SUM(CASE WHEN same_address_flag THEN 1 ELSE 0 END) AS total_same_address      
          FROM cusp_audit.demo.monthly_providers_with_same_address
          WHERE YEAR(CAST(StartOfMonth AS DATE)) = :year2
          GROUP BY provider_licensing_id
        ) s
          ON COALESCE(b.provider_licensing_id, p.provider_licensing_id, d.provider_licensing_id) = s.provider_licensing_id
      ),

      -- Aggregate each metric per year
      unpivoted AS (
        SELECT year, 'total_billed_over_capacity' AS metric, SUM(total_billed_over_capacity) AS total_value FROM combined GROUP BY year
        UNION ALL
        SELECT year, 'total_placed_over_capacity', SUM(total_placed_over_capacity) FROM combined GROUP BY year
        UNION ALL
        SELECT year, 'total_distance_traveled', SUM(total_distance_traveled) FROM combined GROUP BY year
        UNION ALL
        SELECT year, 'total_same_address', SUM(total_same_address) FROM combined GROUP BY year
      )

      -- Rank the metrics and return all top ties
      SELECT year, metric, total_value
      FROM (
        SELECT
          year,
          metric,
          total_value,
          RANK() OVER (PARTITION BY year ORDER BY total_value DESC) AS rnk
        FROM unpivoted
      ) ranked
      WHERE rnk = 1
      ORDER BY year, metric;
    `, 
    namedParameters: {
      year1: String(year1),
      year2: String(year1 - 1),
    },
  };
}