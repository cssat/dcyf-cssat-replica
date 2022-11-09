-- dcyf.sprout_provider_county_lookup source

CREATE MATERIALIZED VIEW dcyf.sprout_provider_county_lookup
TABLESPACE pg_default
AS WITH counties AS (
         SELECT oc."organizationId",
            oc."countyId" AS cd_county,
            co_1.name AS county
           FROM replica."OrganizationCounties" oc
             JOIN replica."Counties" co_1 ON oc."countyId" = co_1.id
        )
 SELECT o.id AS id_provider_sprout,
    co.cd_county,
    co.county,
    now() AS dt_view_refreshed
   FROM dcyf.organizations o
     JOIN counties co ON o.id = co."organizationId"
  ORDER BY o.id, co.cd_county
WITH DATA;
