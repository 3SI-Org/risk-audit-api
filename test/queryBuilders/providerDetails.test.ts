import { describe, expect, it } from "vitest";

import { buildProviderDetailsQuery } from "../../src/queryBuilders/providers/details.js";
import { normalizeSQL } from "../../src/utils.js";

describe("buildProviderDetailsQuery", () => {
  it("should return the correct SQL query and named parameters", () => {
    const params = { provider_licensing_id: "12345" };
    const result = buildProviderDetailsQuery(params);

    expect(normalizeSQL(result.text)).toContain(`SELECT rp.provider_name,`)
    expect(result.namedParameters).toEqual({ plid: "12345" });
  });
});
