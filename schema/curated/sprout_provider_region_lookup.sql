-- dcyf.sprout_provider_region_lookup source

CREATE MATERIALIZED VIEW dcyf.sprout_provider_region_lookup
TABLESPACE pg_default
AS WITH org_region AS (
         SELECT DISTINCT sr."regionId",
            sr."organizationId"
           FROM dcyf.service_referrals sr
          WHERE sr."isCurrentVersion" AND sr."formVersion"::text = 'Ingested'::text
        )
 SELECT o.id AS id_provider_sprout,
    COALESCE(org_reg."regionId", o."regionId") AS cd_region,
    concat('Region ', COALESCE(org_reg."regionId", o."regionId")) AS region,
    now() AS dt_view_refreshed
   FROM dcyf.organizations o
     JOIN org_region org_reg ON o.id = org_reg."organizationId"
  WHERE org_reg."regionId" <> 7
  ORDER BY o.id, (COALESCE(org_reg."regionId", o."regionId"))
WITH DATA;
