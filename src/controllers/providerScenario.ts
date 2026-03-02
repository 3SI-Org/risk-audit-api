import type express from "express";

import { queryData } from "../config/databricks.js";
import { buildBilledOverCapacityQuery } from "../queryBuilders/scenarioQueries/billedOverCapacity.js";
import { buildScenarioDistancTraveledQuery } from "../queryBuilders/scenarioQueries/distanceTraveled.js";
import { buildOverallScoreQuery } from "../queryBuilders/scenarioQueries/overallScore.js";
import { buildPlacedOverCapacityQuery } from "../queryBuilders/scenarioQueries/placedOverCapacity.js";
import { buildScenarioSameAddressQuery } from "../queryBuilders/scenarioQueries/sameAddress.js";

export type ScenarioPlacedOverData = {
  StartOfMonth: string; // ISO DateString
  provider_capacity: number;
  perc_deviation: number;
  placed_over_capacity_flag: boolean;
  before_and_after_school: number;
  part_time: number;
  variable_schedule: number;
  full_time: number;
  subRows: PlacedOverWeek[];
};

export type PlacedOverWeek = {
  hours_open: string;
  hours_close: string;
  child_placements: number;
} & Omit<ScenarioPlacedOverData, "subRows">;

export type UiScenarioMainRows = {
  serviceMonth: string; // ISO DateString
  riskFlag: boolean;
  providerCapacity: number;
  aveWklyPlacements: number; // determine the average from weeks
  percDeviation: number;
  beforeAfterSchool: number;
  partTime: number;
  variableSchedule: number;
  fullTime: number;
  fullTimeOverCap: boolean;
  openTime: string;
  closeTime: string;
  subRows: UiScenarioSubRows[];
};

export type UiScenarioSubRows = Omit<UiScenarioMainRows, "subRows">;

export type ScenarioBilledOverData = {
  StartOfMonth: string; // ISO DateString
  provider_capacity: number;
  billed_child_placements: number;
  perc_deviation: number;
  billed_over_capacity_flag: boolean;
  before_and_after_school: number;
  part_time: number;
  variable_schedule: number;
  full_time: number;
  risk_flag: boolean;
  subRows: BilledOverWeek[];
};

export type BilledOverWeek = {
  hours_open: string;
  hours_close: string;
  billed_child_placements: number;
} & Omit<ScenarioBilledOverData, "subRows">;

type OverallScoreData = {
  startOfMonth: string; // ISO Datestring
  over_billed_capacity: boolean;
  over_placement_capacity: 0 | 1;
  same_address_flag: 0 | 1;
  distance_traveled_flag: 0 | 1;
  total: number; // sum of other columns, true = 1, false = 0
};

type UiOverallScoreData = {
  startOfMonth: string; // ISO Datestring
  overBilledCapacity: 0 | 1;
  overPlacementCapacity: 0 | 1;
  sameAddress: 0 | 1;
  distanceTraveled: 0 | 1;
  total: number; // sum of other columns, true = 1, false = 0
};

export type SameAddressScenarioData = {
  StartOfMonth: string; // ISO DateString
  postal_address: string;
  same_address_flag: boolean;
  provider_licensing_id_match: string;
  provider_name_match: string;
  open_date: string;
  close_date: string;
  subRows: SameAddressScenarioWeek[];
};

export type SameAddressScenarioWeek = Omit<SameAddressScenarioData, "subRows">;

export type UiSameAddressScenarioMainRow = {
  serviceMonth: string;
  postalAddress: string;
  riskFlag: boolean;
  providerId: string;
  providerName: string;
  openDate: string;
  closeDate: string;
  subRows: UiSameAddressScenarioSubRow[];

};
export type UiSameAddressScenarioSubRow = Omit<UiSameAddressScenarioMainRow, "subRows">;

export type DistanceTraveledScenarioData = {
  StartOfMonth: string; // ISO DateString
  family_count: number;
  average_distance_miles: number;
  distance_traveled_flag: boolean;
  subRows: DistanceTraveledScenarioWeek[];
};

export type DistanceTraveledScenarioWeek = Omit<DistanceTraveledScenarioData, "subRows">;

export type UiDistanceTraveledScenarioMainRow = {
  serviceMonth: string;
  riskFlag: boolean;
  distinctEnrolled: number;
  aveDistance: number;
  subRows: UiDistanceTraveledScenarioSubRow[];

};

export type UiDistanceTraveledScenarioSubRow = Omit<UiDistanceTraveledScenarioMainRow, "subRows">;

export function reducePlacedOverWeeks(weeks?: PlacedOverWeek[]): Pick<UiScenarioMainRows, "aveWklyPlacements" | "closeTime" | "openTime"> {
  const averageWeeklyPlacements = (weeks?.reduce((total, current) => (total += current.child_placements), 0)) || 0 / (weeks?.length || 0);
  return {
    aveWklyPlacements: averageWeeklyPlacements,
    openTime: (weeks?.[0]?.hours_open) || "--",
    closeTime: (weeks?.[0]?.hours_close) || "--",
  };
}

export function parsePlacedOverWeeks(weeks: PlacedOverWeek[]): UiScenarioSubRows[] {
  const parsed: UiScenarioSubRows[] = weeks.map((item) => {
    return {
      serviceMonth: item.StartOfMonth,
      riskFlag: item.placed_over_capacity_flag,
      providerCapacity: item.provider_capacity,
      fullTimeOverCap: item.provider_capacity < item.full_time,
      percDeviation: item.perc_deviation,
      beforeAfterSchool: item.before_and_after_school,
      partTime: item.part_time,
      variableSchedule: item.variable_schedule,
      fullTime: item.full_time,
      aveWklyPlacements: item.child_placements,
      openTime: item.hours_open,
      closeTime: item.hours_close,
    };
  });
  return parsed;
}

export async function placedOverCapacityById(req: express.Request, res: express.Response) {
  const provider_licensing_id = String(req.params.providerId);
  const { text, namedParameters } = buildPlacedOverCapacityQuery({ provider_licensing_id });

  try {
    const rawData = await queryData(text, namedParameters) as ScenarioPlacedOverData[];
    // we should parse, top level needs open/close times

    const result: UiScenarioMainRows[] = rawData.map((item) => {
      // combine data from weeks within month for month rows, lift open and close time from week
      const weekly = reducePlacedOverWeeks(item.subRows);
      // handle convert from domain model to ui model
      const subRows = parsePlacedOverWeeks(item.subRows);
      return {
        serviceMonth: item.StartOfMonth,
        riskFlag: item.placed_over_capacity_flag,
        providerCapacity: item.provider_capacity,
        fullTimeOverCap: item.provider_capacity < item.full_time,
        percDeviation: item.perc_deviation,
        beforeAfterSchool: item.before_and_after_school,
        partTime: item.part_time,
        variableSchedule: item.variable_schedule,
        fullTime: item.full_time,
        subRows,
        ...weekly,
      };
    });

    res.json(result);
  }
  catch (err: any) {
    // console.log("err =======", err);
    res.status(500).json({ error: err.message });
  }
}

export function reduceBilledOverWeeks(weeks?: BilledOverWeek[]): Pick<UiScenarioMainRows, "aveWklyPlacements" | "closeTime" | "openTime"> {
  const averageWeeklyPlacements = ((weeks?.reduce((total, current) => (total += current.billed_child_placements), 0)) || 0) / (weeks?.length || 0);
  return {
    aveWklyPlacements: averageWeeklyPlacements,
    openTime: (weeks?.[0]?.hours_open) || "--",
    closeTime: (weeks?.[0]?.hours_close) || "--",
  };
}

export function parseBilledOverWeeks(weeks: BilledOverWeek[]): UiScenarioSubRows[] {
  const parsed: UiScenarioSubRows[] = weeks?.map((item) => {
    return {
      serviceMonth: item.StartOfMonth,
      riskFlag: item.risk_flag,
      providerCapacity: item.provider_capacity,
      fullTimeOverCap: item.provider_capacity < item.full_time,
      percDeviation: item.perc_deviation,
      beforeAfterSchool: item.before_and_after_school,
      partTime: item.part_time,
      variableSchedule: item.variable_schedule,
      fullTime: item.full_time,
      aveWklyPlacements: item.billed_child_placements,
      openTime: item.hours_open,
      closeTime: item.hours_close,
    };
  }) || [];
  return parsed;
}

export async function billedOverCapacityById(req: express.Request, res: express.Response) {
  const provider_licensing_id = String(req.params.providerId);
  const { text, namedParameters } = buildBilledOverCapacityQuery({ provider_licensing_id });

  try {
    const rawData = await queryData(text, namedParameters) as ScenarioBilledOverData[];
    // we should parse, top level needs open/close times

    const result: UiScenarioMainRows[] = rawData.map((item) => {
      // combine data from weeks within month for month rows, lift open and close time from week
      const weekly = reduceBilledOverWeeks(item.subRows);
      // handle convert from domain model to ui model
      const subRows = parseBilledOverWeeks(item.subRows);
      return {
        serviceMonth: item.StartOfMonth,
        riskFlag: item.billed_over_capacity_flag,
        providerCapacity: item.provider_capacity,
        fullTimeOverCap: item.provider_capacity < item.full_time,
        percDeviation: item.perc_deviation,
        beforeAfterSchool: item.before_and_after_school,
        partTime: item.part_time,
        variableSchedule: item.variable_schedule,
        fullTime: item.full_time,
        subRows,
        ...weekly,
      };
    });

    res.json(result);
  }
  catch (err: any) {
    // console.log("err =======", err);
    res.status(500).json({ error: err.message });
  }
}

export async function overallScoreById(req: express.Request, res: express.Response) {
  const provider_licensing_id = String(req.params.providerId);
  const { text, namedParameters } = buildOverallScoreQuery({ provider_licensing_id });

  try {
    const rawData = await queryData(text, namedParameters) as OverallScoreData[];

    const result: UiOverallScoreData[] = rawData.map((item): UiOverallScoreData => {
      return {
        startOfMonth: item.startOfMonth,
        sameAddress: item.same_address_flag || 0,
        overBilledCapacity: item.over_billed_capacity ? 1 : 0, // this comes as a boolean convert here
        overPlacementCapacity: item.over_placement_capacity || 0,
        distanceTraveled: item.distance_traveled_flag || 0,
        total: item.total,
      };
    });

    res.json(result);
  }
  catch (err: any) {
    // console.log("err =======", err);
    res.status(500).json({ error: err.message });
  }
}

export function parseSameAddressWeeks(week: SameAddressScenarioWeek[]): UiSameAddressScenarioSubRow[] {
  const result: UiSameAddressScenarioSubRow[] = week?.map((item) => {
    return {
      serviceMonth: item.StartOfMonth,
      riskFlag: item.same_address_flag,
      postalAddress: item.postal_address,
      providerId: item.provider_licensing_id_match,
      providerName: item.provider_name_match,
      openDate: item.open_date || "--",
      closeDate: item.close_date || "--",
    };
  }) || [];
  return result;
}

export async function sameAddressById(req: express.Request, res: express.Response) {
  const provider_licensing_id = String(req.params.providerId);
  const { text, namedParameters } = buildScenarioSameAddressQuery({ provider_licensing_id });

  try {
    const rawData = await queryData(text, namedParameters) as SameAddressScenarioData[];
    const result: UiSameAddressScenarioMainRow[] = rawData.map((item) => {
      // handle convert from domain model to ui model
      const subRows = parseSameAddressWeeks(item.subRows);
      return {
        serviceMonth: item.StartOfMonth,
        riskFlag: item.same_address_flag,
        postalAddress: item.postal_address,
        providerId: item.provider_licensing_id_match,
        providerName: item.provider_name_match,
        openDate: item.open_date || "--",
        closeDate: item.close_date || "--",
        subRows,
      };
    });

    res.json(result);
  }
  catch (err: any) {
    // console.log("err =======", err);
    res.status(500).json({ error: err.message });
  }
}

function reduceDistanceWeeks(weeks: DistanceTraveledScenarioWeek[]) {
  const monthlyTotal = weeks.reduce((total, week) => {
    return total += week.average_distance_miles;
  }, 0);

  return {
    aveDistance: monthlyTotal / weeks.length,
  };
}

export function parseDistanceWeeks(week: DistanceTraveledScenarioWeek[]): UiDistanceTraveledScenarioSubRow[] {
  const result: UiDistanceTraveledScenarioSubRow[] = week?.map((item) => {
    return {
      serviceMonth: item.StartOfMonth,
      riskFlag: item.distance_traveled_flag,
      distinctEnrolled: item.family_count,
      aveDistance: item.average_distance_miles,
    };
  }) || [];
  return result;
}

export async function distanceTraveledById(req: express.Request, res: express.Response) {
  const provider_licensing_id = String(req.params.providerId);
  const { text, namedParameters } = buildScenarioDistancTraveledQuery({ provider_licensing_id });

  try {
    const rawData = await queryData(text, namedParameters) as DistanceTraveledScenarioData[];
    // we should parse, top level needs open/close times

    const result: UiDistanceTraveledScenarioMainRow[] = rawData.map((item) => {
      const monthlyAverage = reduceDistanceWeeks(item.subRows);
      // handle convert from domain model to ui model
      const subRows = parseDistanceWeeks(item.subRows);
      return {
        serviceMonth: item.StartOfMonth,
        riskFlag: item.distance_traveled_flag,
        distinctEnrolled: item.family_count,
        aveDistance: monthlyAverage.aveDistance,

        subRows,
      };
    });

    res.json(result);
  }
  catch (err: any) {
    // console.log("err =======", err);
    res.status(500).json({ error: err.message });
  }
}
