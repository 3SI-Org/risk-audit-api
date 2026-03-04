import request from "supertest";
import { describe, expect, it, vi } from "vitest";

import type { BilledOverWeek, DistanceTraveledScenarioWeek, PlacedOverWeek, SameAddressScenarioWeek, ScenarioBilledOverData, UiDistanceTraveledScenarioSubRow, UiSameAddressScenarioSubRow, UiScenarioSubRows } from "../src/controllers/providerScenario.js";

import app from "../src/app.js";
import { parseBilledOverWeeks, parseDistanceWeeks, parsePlacedOverWeeks, parseSameAddressWeeks, reduceBilledOverWeeks, reducePlacedOverWeeks } from "../src/controllers/providerScenario.js";

const mockQueryData = vi.hoisted(() => vi.fn());

vi.mock("./../src/config/databricks.js", () => ({
  queryData: mockQueryData,
}));

describe("Helper functions in providerScenario", () => {
  describe("parsePlacedOverWeeks", () => {
    it("should parse weeks into UiScenarioSubRows correctly", () => {
      const week: PlacedOverWeek[] = [
        {
          StartOfMonth: "2026-01-01",
          hours_open: "08:00",
          hours_close: "17:00",
          child_placements: 10,
          provider_capacity: 5,
          perc_deviation: 1.0,
          before_and_after_school: 2,
          part_time: 3,
          variable_schedule: 4,
          full_time: 6,
          placed_over_capacity_flag: false,
        },
      ];

      const expected: UiScenarioSubRows[] = [
        {
          serviceMonth: "2026-01-01",
          riskFlag: false,
          providerCapacity: 5,
          fullTimeOverCap: true,
          percDeviation: 1.0,
          beforeAfterSchool: 2,
          partTime: 3,
          variableSchedule: 4,
          fullTime: 6,
          aveWklyPlacements: 10,
          openTime: "08:00",
          closeTime: "17:00",
        },
      ];

      const result = parsePlacedOverWeeks(week);
      expect(result).toEqual(expected);
    });
  });

  describe("reducePlacedOverWeeks", () => {
    it("should calculate average weekly placements and return open/close times correctly", () => {
      const week: PlacedOverWeek[] = [
        {
          StartOfMonth: "2026-01-01",
          hours_open: "08:00",
          hours_close: "17:00",
          child_placements: 10,
          provider_capacity: 5,
          perc_deviation: 1.0,
          before_and_after_school: 2,
          part_time: 3,
          variable_schedule: 4,
          full_time: 6,
          placed_over_capacity_flag: false,
        },
      ];

      const expected = {
        aveWklyPlacements: 10,
        openTime: "08:00",
        closeTime: "17:00",
      };

      const result = reducePlacedOverWeeks(week);
      expect(result).toEqual(expected);
    });
  });

  describe("placedOverCapacityById", () => {
    it("should return the correct response for a given providerId", async () => {
      const mockProviderData = [
        {
          StartOfMonth: "2026-01-01",
          provider_capacity: 5,
          perc_deviation: 1.0,
          placed_over_capacity_flag: false,
          before_and_after_school: 2,
          part_time: 3,
          variable_schedule: 4,
          full_time: 6,
          subRows: [
            {
              StartOfMonth: "2026-01-01",
              hours_open: "08:00",
              hours_close: "17:00",
              child_placements: 10,
              provider_capacity: 5,
              perc_deviation: 1.0,
              placed_over_capacity_flag: false,
              before_and_after_school: 2,
              part_time: 3,
              variable_schedule: 4,
              full_time: 6,
            },
          ],
        },
      ]

      mockQueryData.mockResolvedValue(mockProviderData);
      const res = await request(app).get("/api/v1/scenario/placed/123");

      expect(res.status).toBe(200);
      console.log(res.body);
      expect(res.body).toEqual([
        {
          serviceMonth: "2026-01-01",
          riskFlag: false,
          providerCapacity: 5,
          fullTimeOverCap: true,
          percDeviation: 1.0,
          beforeAfterSchool: 2,
          partTime: 3,
          variableSchedule: 4,
          fullTime: 6,
          aveWklyPlacements: 10,
          openTime: "08:00",
          closeTime: "17:00",
          subRows: [
            {
              serviceMonth: "2026-01-01",
              riskFlag: false,
              providerCapacity: 5,
              fullTimeOverCap: true,
              percDeviation: 1.0,
              beforeAfterSchool: 2,
              partTime: 3,
              variableSchedule: 4,
              fullTime: 6,
              aveWklyPlacements: 10,
              openTime: "08:00",
              closeTime: "17:00",
            },
          ],
        },
      ]);
    });
  });

  describe("parseBilledOverWeeks", () => {
    it("should parse weeks into UiScenarioSubRows correctly for billed data", () => {
      const week: BilledOverWeek[] = [
        {
          StartOfMonth: "2026-01-01",
          hours_open: "08:00",
          hours_close: "17:00",
          billed_child_placements: 15,
          provider_capacity: 5,
          perc_deviation: 2.0,
          before_and_after_school: 3,
          part_time: 4,
          variable_schedule: 5,
          full_time: 7,
          risk_flag: false,
          billed_over_capacity_flag: false,
        },
      ];

      const expected: UiScenarioSubRows[] = [
        {
          serviceMonth: "2026-01-01",
          riskFlag: false,
          providerCapacity: 5,
          fullTimeOverCap: true,
          percDeviation: 2.0,
          beforeAfterSchool: 3,
          partTime: 4,
          variableSchedule: 5,
          fullTime: 7,
          aveWklyPlacements: 15,
          openTime: "08:00",
          closeTime: "17:00",
        },
      ];

      const result = parseBilledOverWeeks(week);
      expect(result).toEqual(expected);
    });
  });

  describe("reduceBilledOverWeeks", () => {
    it("should calculate average weekly placements and return open/close times correctly for billed data", () => {
      const week: BilledOverWeek[] = [
        {
          StartOfMonth: "2026-01-01",
          hours_open: "08:00",
          hours_close: "17:00",
          billed_child_placements: 15,
          provider_capacity: 5,
          perc_deviation: 2.0,
          before_and_after_school: 3,
          part_time: 4,
          variable_schedule: 5,
          full_time: 7,
          risk_flag: false,
          billed_over_capacity_flag: false,
        },
      ];

      const expected = {
        aveWklyPlacements: 15,
        openTime: "08:00",
        closeTime: "17:00",
      };

      const result = reduceBilledOverWeeks(week);
      expect(result).toEqual(expected);
    });
  });

  describe("billedOverCapacityById", () => {
    it("should return the correct response for a given providerId for billed data", async () => {
      const mockProviderData: ScenarioBilledOverData[] = [
        {
          StartOfMonth: "2026-01-01",
          provider_capacity: 5,
          perc_deviation: 2.0,
          billed_child_placements: 15,
          billed_over_capacity_flag: false,
          before_and_after_school: 3,
          part_time: 4,
          variable_schedule: 5,
          full_time: 7,
          risk_flag: false,
          subRows: [
            {
              StartOfMonth: "2026-01-01",
              provider_capacity: 5,
              perc_deviation: 2.0,
              billed_child_placements: 15,
              billed_over_capacity_flag: false,
              before_and_after_school: 3,
              part_time: 4,
              variable_schedule: 5,
              full_time: 7,
              risk_flag: false,
              hours_open: "08:00",
              hours_close: "17:00",
            }
          ],
        },
      ];


      mockQueryData.mockResolvedValue(mockProviderData);
      const res = await request(app).get("/api/v1/scenario/billed/123");

      expect(res.status).toBe(200);
      expect(res.body).toEqual([
        {
          serviceMonth: "2026-01-01",
          riskFlag: false,
          providerCapacity: 5,
          fullTimeOverCap: true,
          percDeviation: 2.0,
          beforeAfterSchool: 3,
          partTime: 4,
          variableSchedule: 5,
          fullTime: 7,
          aveWklyPlacements: 15,
          openTime: "08:00",
          closeTime: "17:00",
          subRows: [
            {
              serviceMonth: "2026-01-01",
              riskFlag: false,
              providerCapacity: 5,
              fullTimeOverCap: true,
              percDeviation: 2.0,
              beforeAfterSchool: 3,
              partTime: 4,
              variableSchedule: 5,
              fullTime: 7,
              aveWklyPlacements: 15,
              openTime: "08:00",
              closeTime: "17:00",
            },
          ],
        },
      ]);
    });
  });

  describe("parseSameAddressWeeks", () => {
    it("should parse weeks into UiSameAddressScenarioSubRow correctly for same address data", () => {
      const week: SameAddressScenarioWeek[] = [
        {
          StartOfMonth: "2026-01-01",
          same_address_flag: true,
          postal_address: "123 Main St",
          provider_licensing_id_match: "456",
          provider_name_match: "Example Provider",
          open_date: "2026-01-01",
          close_date: "2026-12-31",
        },
      ];

      const expected: UiSameAddressScenarioSubRow[] = [
        {
          serviceMonth: "2026-01-01",
          riskFlag: true,
          postalAddress: "123 Main St",
          providerId: "456",
          providerName: "Example Provider",
          openDate: "2026-01-01",
          closeDate: "2026-12-31",
        },
      ];

      const result = parseSameAddressWeeks(week);
      expect(result).toEqual(expected);
    });
  });

  describe("parseDistanceWeeks", () => {
    it("should parse weeks into UiDistanceTraveledScenarioSubRow correctly for distance data", () => {
      const week: DistanceTraveledScenarioWeek[] = [
        {
          StartOfMonth: "2026-01-01",
          family_count: 3,
          average_distance_miles: 5.5,
          distance_traveled_flag: true,
        },
      ];

      const expected: UiDistanceTraveledScenarioSubRow[] = [
        {
          serviceMonth: "2026-01-01",
          riskFlag: true,
          distinctEnrolled: 3,
          aveDistance: 5.5,
        },
      ];

      const result = parseDistanceWeeks(week);
      expect(result).toEqual(expected);
    });
  });
});
