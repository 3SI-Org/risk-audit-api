import type { ExecuteStatementOptions } from "@databricks/sql/dist/contracts/IDBSQLSession.js";

import { DBSQLClient } from "@databricks/sql";

import { env } from "../env.js";

let client: DBSQLClient | null = null;

export async function getDatabricksClient(): Promise<DBSQLClient> {
  if (!client) {
    client = new DBSQLClient();
    await client.connect({
      host: env.DATABRICKS_HOST!,
      path: env.DATABRICKS_HTTP_PATH!,
      token: env.PAT_TOKEN!,
    });
    console.log("Databricks client connected");
  }
  return client;
}

export async function queryData(
  sql: string,
  namedParameters?: ExecuteStatementOptions["namedParameters"],
) {
  const client = await getDatabricksClient();
  const session = await client.openSession();
  try {
    const operation = await session.executeStatement(sql, { namedParameters });
    const result = await operation.fetchAll();
    await operation.close();
    return result;
  }
  finally {
    await session.close().catch(() => {});
  }
};

export function resetClient() {
  client = null;
}
