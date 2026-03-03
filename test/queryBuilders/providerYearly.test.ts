import { describe, expect, it } from "vitest";

import type { ProviderFilters } from "../../src/controllers/providerFilters.js";

import { buildProviderYearlyQuery } from "../../src/queryBuilders/providers/yearly.js";

describe("buildProviderYearlyQuery", () => {
  it("should return a valid SQL query with named parameters", () => {
    const params: ProviderFilters = {
      flagStatus: true,
      year: "2023",
      offset: "0",
      cities: ["New York", "Los Angeles"],
    };

    const result = buildProviderYearlyQuery(params);

    expect(result.text).toContain("WITH combined AS");
    expect(result.text).toContain("ORDER BY c.overall_risk_score DESC, c.provider_licensing_id");
    expect(result.namedParameters).toEqual({
      year: "2023",
      offset: 0,
      flagStatus: true,
      cities: "New York,Los Angeles",
    });
  });
});
