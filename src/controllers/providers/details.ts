import type express from "express";

import { queryData } from "../../config/databricks.js";
import { buildProviderDetailsQuery } from "../../queryBuilders/providers/details.js";

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

export async function getProviderDetails(req: express.Request, res: express.Response) {
  const provider_licensing_id = req.params.providerId;
  const { text, namedParameters } = buildProviderDetailsQuery({ provider_licensing_id });

  try {
    const rawData = await queryData(text, namedParameters) as ProviderDetailsData[];
    const result: UiProviderDetailsData[] = rawData.map((item) => {
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
    });
    res.json(result[0]);
  }
  catch (err: any) {
    res.status(500).json({ error: err.message });
  }
}
