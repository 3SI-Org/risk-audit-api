import SQL from "sql-template-strings";

type BuildProviderDetailsQueryParams = {
  provider_licensing_id: string;
};

export function buildProviderDetailsQuery({ provider_licensing_id }: BuildProviderDetailsQueryParams) {
  const plid = String(provider_licensing_id)
  const query= SQL`
    SELECT
      rp.provider_name,
      rp.provider_licensing_id,
      rp.provider_status,
      rp.provider_type,
      rp.provider_phone,
      rp.provider_email,
      a.postal_address,
      a.city,
      a.zip
    FROM cusp_audit.demo.risk_providers rp
    JOIN cusp_audit.fake_data.addresses a ON rp.provider_address_uid = a.provider_address_uid
    WHERE rp.provider_licensing_id = :plid
  `;

  const namedParameters = {
    plid,
  };

  return { text: query.text, namedParameters };
}
