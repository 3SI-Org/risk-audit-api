import type express from "express";

import { queryData } from "../../config/databricks.js";
import { buildFlaggedCountQuery, buildHighestRiskScoreQuery, buildHighRiskCountQuery, buildProviderCountQuery } from "../../queryBuilders/providers/overview.js";

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
  const { text, namedParameters } = buildHighestRiskScoreQuery(req);

  try {
    const data = await queryData(text, namedParameters);
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
  const { text, namedParameters } = buildHighRiskCountQuery(req);

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

  const { text } = buildFlaggedCountQuery();

  try {
    const data = await queryData(text);
    res.json(data[0]);
  }
  catch (err: any) {
    // console.log("err =======", err);
    res.status(500).json({ error: err.message });
  }
}
