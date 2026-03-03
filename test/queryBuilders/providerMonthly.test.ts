import { describe, expect, it } from "vitest";

import { checkedFilter } from "../../src/queryBuilders/providers/monthly.js";

describe("checkedFilter", () => {
  it("returns true when flagged is true and unflagged is false", () => {
    const filterOptions = { flagged: true, unflagged: false };
    expect(checkedFilter(filterOptions)).toBe(true);
  });

  it("returns false when flagged is false and unflagged is true", () => {
    const filterOptions = { flagged: false, unflagged: true };
    expect(checkedFilter(filterOptions)).toBe(false);
  });

  it("returns null when both flagged and unflagged are true", () => {
    const filterOptions = { flagged: true, unflagged: true };
    expect(checkedFilter(filterOptions)).toBe(null);
  });

  it("returns null when both flagged and unflagged are false", () => {
    const filterOptions = { flagged: false, unflagged: false };
    expect(checkedFilter(filterOptions)).toBe(null);
  });
});
