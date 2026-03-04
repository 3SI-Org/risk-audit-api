export const normalizeSQL = (sql: string) => sql.replace(/\s+/g, ' ').trim();

export function toCsv<T extends Record<string, any>>(rows: T[], headers: (keyof T)[]): string {
  const escape = (val: any) => {
    if (val === null || val === undefined) return "";
    const str = String(val);
    return /[",\n]/.test(str) ? `"${str.replace(/"/g, '""')}"` : str;
  };

  return [
    (headers as string[]).join(","),
    ...rows.map(row => headers.map(h => escape(row[h])).join(",")),
  ].join("\n");
}

export function parseYear(value?: string): number | null {
  const yearNum = Number.parseInt(value ?? "", 10);
  if (Number.isNaN(yearNum) || yearNum < 1980 || yearNum > 2100) return null;
  return yearNum;
}