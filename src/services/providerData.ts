// services/providerData.ts

import type { AnnualProviderData, MonthlyProviderData, ProviderDetailsData, ProviderFilters } from "../types/provider.js";
import type { UiAnnualProviderData, UiMonthlyProviderData, UiProviderDetailsData } from "../types/uiProvider.js";

import { buildProviderDetailsQuery } from "../queryBuilders/providers/details.js";
import { buildProviderMonthlyQuery } from "../queryBuilders/providers/monthly.js";
import { buildProviderYearlyQuery } from "../queryBuilders/providers/yearly.js";

type QueryFn = typeof import("../config/databricks.js").queryData;

export function createProviderDataService(queryData: QueryFn) {
  // ── Details ────────────────────────────────────────────

  async function getProviderDetails(providerId: string): Promise<UiProviderDetailsData | null> {
    const { text, namedParameters } = buildProviderDetailsQuery({ provider_licensing_id: providerId });
    const rawData = await queryData(text, namedParameters) as ProviderDetailsData[];

    if (!rawData.length)
      return null;

    return transformDetailsData(rawData[0]);
  }

  // ── Monthly ────────────────────────────────────────────

  async function getMonthlyData(filters: ProviderFilters): Promise<UiMonthlyProviderData[]> {
    const { text, namedParameters } = buildProviderMonthlyQuery(filters);
    const rawData = await queryData(text, namedParameters) as MonthlyProviderData[];
    return transformMonthlyData(rawData);
  }

  // ── Yearly ─────────────────────────────────────────────

  async function getYearlyData(filters: ProviderFilters): Promise<UiAnnualProviderData[]> {
    const { text, namedParameters } = buildProviderYearlyQuery(filters);
    const rawData = await queryData(text, namedParameters) as AnnualProviderData[];
    return transformYearlyData(rawData);
  }

  return {
    getProviderDetails,
    getMonthlyData,
    getYearlyData,
  };
}

// ── Transforms (private) ─────────────────────────────────

function transformDetailsData(item: ProviderDetailsData): UiProviderDetailsData {
  return {
    providerLicensingId: item.provider_licensing_id,
    providerName: item.provider_name,
    postalAddress: item.postal_address || "--",
    city: item.city || "--",
    zip: item.zip || "--",
    providerPhone: item.provider_phone || "--",
    providerEmail: item.provider_email || "--",
    providerStatus: item.provider_status || "--",
    providerType: item.provider_type || "--",
  };
}

function transformMonthlyData(rawData: MonthlyProviderData[]): UiMonthlyProviderData[] {
  return rawData.map(item => ({
    providerLicensingId: item.provider_licensing_id,
    startOfMonth: item.StartOfMonth,
    providerName: item.provider_name,
    childrenBilledOverCapacity: item.over_billed_capacity ? "Yes" : "--",
    childrenPlacedOverCapacity: item.over_placement_capacity ? "Yes" : "--",
    distanceTraveled: item.distance_traveled_flag ? "Yes" : "--",
    providersWithSameAddress: item.same_address_flag ? "Yes" : "--",
    overallRiskScore: item.total || 0,
    flagged: item?.is_flagged || false,
    comment: item?.comment || "",
    postalAddress: item.postal_address || "--",
    city: item.city || "--",
    zip: item.zip || "--",
  }));
}

function transformYearlyData(rawData: AnnualProviderData[]): UiAnnualProviderData[] {
  return rawData.map(item => ({
    providerLicensingId: item.provider_licensing_id,
    providerName: item.provider_name || "--",
    childrenBilledOverCapacity: item.total_billed_over_capacity || 0,
    childrenPlacedOverCapacity: item.total_placed_over_capacity || 0,
    distanceTraveled: item.total_distance_traveled || 0,
    providersWithSameAddress: item.total_same_address || 0,
    overallRiskScore: item.overall_risk_score || 0,
    flagged: item?.is_flagged || false,
    comment: item?.comment || "",
    postalAddress: item?.postal_address || "--",
    city: item?.city || "--",
    zip: item?.zip || "--",
    capacityLicensed: item?.capacity_licensed || "--",
    providerFacilityType: item?.provider_facility_type || "--",
    providerStatus: item?.provider_status || "--",
  }));
}
