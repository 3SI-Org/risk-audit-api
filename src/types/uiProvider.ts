export type UiProviderDetailsData = {
  providerLicensingId: string;
  providerName: string;
  postalAddress: string;
  city: string;
  zip: string;
  providerStatus: string;
  providerType: string;
  providerEmail: string;
  providerPhone: string;
};

export type UiMonthlyProviderData = {
  providerLicensingId: string;
  providerName: string;
  overallRiskScore: number;
  childrenBilledOverCapacity: string;
  childrenPlacedOverCapacity: string;
  distanceTraveled: string;
  providersWithSameAddress: string;
  flagged?: boolean;
  comment?: string;
  startOfMonth?: string;
  postalAddress: string;
  city: string;
  zip: string;
};

export type UiAnnualProviderData = {
  providerLicensingId: string;
  providerName: string;
  childrenBilledOverCapacity: number;
  childrenPlacedOverCapacity: number;
  distanceTraveled: number;
  overallRiskScore: number;
  providersWithSameAddress: number;
  flagged: boolean;
  comment: string;
  postalAddress: string;
  city: string;
  zip: string;
};