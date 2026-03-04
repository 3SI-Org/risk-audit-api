import { describe, expect, it } from "vitest";

import { buildPlacedOverCapacityQuery } from "../../../src/queryBuilders/scenarioQueries/placedOverCapacity.js";
import { normalizeSQL } from "../../../src/utils.js";

describe("buildPlacedOverCapacityQuery", () => {
  it("returns the correct query and named parameters", () => {
    const params = { provider_licensing_id: "12345" };
    const result = buildPlacedOverCapacityQuery(params);

    expect(result).toHaveProperty("text");
    expect(normalizeSQL(result.text)).toContain(`SELECT m.StartOfMonth,`);

    expect(result.namedParameters).toEqual({ plid: "12345" });
  });
});
