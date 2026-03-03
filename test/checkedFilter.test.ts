import { describe, expect, it } from "vitest";

import { checkedFilter } from "../src/queryBuilders/providers/monthly.js";

describe("checkedFilter", () => {
  it("returns true when flagged is true and unflagged is false", () => {
    const body = { flagged: true, unflagged: false };
    const checked = checkedFilter(body);
    expect(checked).toBe(true);
  });

  it("returns false when flagged is false and unflagged is true", () => {
    const body = { flagged: false, unflagged: true };
    const checked = checkedFilter(body);
    expect(checked).toBe(false);
  });

  it("returns null when both flagged and unflagged are true", () => {
    const body = { flagged: true, unflagged: true };
    const checked = checkedFilter(body);
    expect(checked).toBeNull();
  });

  it("returns null when both flagged and unflagged are false", () => {
    const body = { flagged: false, unflagged: false };
    const checked = checkedFilter(body);
    expect(checked).toBeNull();
  });
});
