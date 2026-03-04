export type ProviderDetailsData = {
  provider_licensing_id: string;
  provider_name: string;
  postal_address: string;
  city: string;
  zip: string;
  provider_status: string;
  provider_type: string;
  provider_email: string;
  provider_phone: string;
};

export type MonthlyProviderData = {
  provider_licensing_id: string;
  provider_name: string;
  StartOfMonth: string;
  over_billed_capacity: boolean;
  over_placement_capacity: boolean;
  same_address_flag: boolean;
  distance_traveled_flag: boolean;
  total: number;
  is_flagged: boolean;
  comment: string;
  postal_address: string;
  city: string;
  zip: string;
};

export type AnnualProviderData = {
  capacity_licensed: string;
  provider_facility_type: string;
  provider_status: string;
  provider_licensing_id: string;
  provider_name: string;
  StartOfMonth: string; // ISO DateString
  total_billed_over_capacity: number;
  total_placed_over_capacity: number;
  total_same_address: number;
  total_distance_traveled: number;
  overall_risk_score: number;
  is_flagged: boolean;
  comment: string;
  postal_address: string;
  city: string;
  zip: string;
};

export type ProviderFilters = {
  year: string;
  month: string;
  offset: string;
  flagStatus: boolean | null;
  cities: string[];
  licenseCapacity: string;
};

export type FilterConfig = {
  name: string;
  selectExpr: string;
  nullFilter: string;
  searchable: boolean;
  facetType?: "distinct" | "range";
  applyWhere: (sqlQuery: any, params: ProviderFilters) => void;
  applyParams: (params: ProviderFilters, named: Record<string, any>) => void;
  isActive: (params: ProviderFilters) => boolean;
};
