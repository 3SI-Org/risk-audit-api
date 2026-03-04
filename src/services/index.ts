import { queryData } from "../config/databricks.js";
import { createProviderDataService } from "./providerData.js";

export const providerDataService = createProviderDataService(queryData);