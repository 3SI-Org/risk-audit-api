// @desc    Update provider insight data - update comment or update flag status
// @route   put /api/v1/providerData/insights/:id

import type express from "express";

import { queryData } from "../config/databricks.js";

// @access  Private
export async function updateProviderDataInsights(req: express.Request, res: express.Response) {
  // const body = req.body;
  const { actionType } = req?.body;
  let sqlQuery = "";
  let historicalQuery = "";

  if (actionType === "CREATE") {
    const { provider_licensing_id, is_flagged, comment, created_at } = req.body;
    sqlQuery = `
      MERGE INTO cusp_audit.demo.provider_insights AS target
      USING (
        SELECT '${provider_licensing_id}' AS provider_licensing_id,
        '${is_flagged}' AS is_flagged,
        '${comment}' AS comment,
        '${created_at}' AS created_at
        ) AS source
      ON target.provider_licensing_id = source.provider_licensing_id
      WHEN MATCHED THEN
      UPDATE SET 
          target.provider_licensing_id = source.provider_licensing_id,
          target.is_flagged = source.is_flagged,
          target.comment = source.comment,
          target.created_at = source.created_at
      WHEN NOT MATCHED THEN
      INSERT (provider_licensing_id, is_flagged, comment, created_at)
      VALUES (source.provider_licensing_id, source.is_flagged, source.comment, source.created_at);`;

    historicalQuery = `
      INSERT INTO cusp_audit.demo.provider_insights_history (
        provider_insight_id,
        created_at,
        created_by,
        action_taken,
        comment,
        is_active
      )
      SELECT
        id,
        '${created_at}',
        created_by,
        '${actionType}',
        comment,
        is_flagged
      FROM cusp_audit.demo.provider_insights
      WHERE provider_licensing_id = '${provider_licensing_id}';
    `;
  }

  if (actionType === "UPDATE") {
    const { provider_licensing_id, comment, created_at } = req.body;
    sqlQuery = `
    MERGE INTO cusp_audit.demo.provider_insights AS target
      USING (
      SELECT
        '${provider_licensing_id}' AS provider_licensing_id,
        '${comment}' AS comment
      ) AS source
      ON target.provider_licensing_id = source.provider_licensing_id
      WHEN MATCHED THEN
      UPDATE SET
        comment = source.comment;`;

    historicalQuery = `
      INSERT INTO cusp_audit.demo.provider_insights_history (
        provider_insight_id,
        created_at,
        created_by,
        action_taken,
        comment,
        is_active
      )
      SELECT
        id,
        '${created_at}',
        created_by,
        '${actionType}',
        comment,
        is_flagged
      FROM cusp_audit.demo.provider_insights
      WHERE provider_licensing_id = '${provider_licensing_id}';
      `;
  }

  if (actionType === "RESOLVE") {
    const { provider_licensing_id, is_flagged, comment, resolved_on } = req.body;
    sqlQuery = `
      MERGE INTO cusp_audit.demo.provider_insights AS target
      USING (
        SELECT '${provider_licensing_id}' AS provider_licensing_id,
        '${is_flagged}' AS is_flagged,
        '${comment}' AS comment,
        '${resolved_on}' AS resolved_on
        ) AS source
      ON target.provider_licensing_id = source.provider_licensing_id
      WHEN MATCHED THEN
      UPDATE SET 
          target.is_flagged = source.is_flagged,
          target.comment = '',
          target.resolved_on = null`;

    historicalQuery = `
      INSERT INTO cusp_audit.demo.provider_insights_history (
        provider_insight_id,
        created_at,
        created_by,
        action_taken,
        comment,
        is_active
      )
      SELECT
        id,
        '${resolved_on}',
        created_by,
        '${actionType}',
        '${comment}',
        is_flagged
      FROM cusp_audit.demo.provider_insights
      WHERE provider_licensing_id = '${provider_licensing_id}';
      `;
  }

  // update & create

  // ADD TO HISTORY TOO

  //   'select * from cusp_audit.demo limit 10'
  try {
    const data = await queryData(sqlQuery);
    await queryData(historicalQuery);

    // get the value back and spin off the createHistoryRecord with the id from the new row

    res.json(data);
  }
  catch (err: any) {
    // console.log("err =======", err);
    res.status(500).json({ error: err.message });
  }
}

export async function getProviderDataInsights(req: express.Request, res: express.Response) {
  const provider_id = req.params.providerId;
  const sqlQuery = `
  SELECT
    p.provider_licensing_id,
    p.id,
    p.resolved_on,
    p.is_flagged,
    p.comment,
    p.created_at,
    p.created_by,
    filter(
    collect_list(
      named_struct(
        'id', h.id,
        'action', h.action_taken,
        'comment', h.comment,
        'is_active', h.is_active,
        'created_at', h.created_at,
        'created_by', h.created_by
      )
    ),
    item -> item.id IS NOT NULL
    ) AS history
  FROM cusp_audit.demo.provider_insights p
  LEFT JOIN cusp_audit.demo.provider_insights_history h ON h.provider_insight_id = p.id
  WHERE p.provider_licensing_id = '${provider_id}'
  GROUP BY p.id, p.provider_licensing_id, p.is_flagged, p.resolved_on, p.created_by, p.comment, p.created_at;`;

  try {
    const data = await queryData(sqlQuery);
    if (data?.length === 1) {
      res.json(data[0]);
    }
    else {
      res.json(data);
    }
  }
  catch (err: any) {
    console.log("err =======", err);
    res.status(500).json({ error: err.message });
  }
}
