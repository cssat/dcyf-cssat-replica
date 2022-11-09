-- dcyf.sprout_provider_office_lookup source

CREATE MATERIALIZED VIEW dcyf.sprout_provider_office_lookup
TABLESPACE pg_default
AS WITH offices1 AS (
         SELECT DISTINCT sr."organizationId",
            sr."dshsOffice"
           FROM dcyf.service_referrals sr
          WHERE sr."isCurrentVersion" AND sr."formVersion"::text = 'Ingested'::text
        ), offices AS (
         SELECT DISTINCT offices1."organizationId",
            "substring"(offices1."dshsOffice"::text, '\(([0-9].*)\)'::text)::integer AS cd_office,
            offices1."dshsOffice" AS office
           FROM offices1
        )
 SELECT o.id AS id_provider_sprout,
    offi.cd_office,
    offi.office,
    now() AS dt_view_refreshed
   FROM dcyf.organizations o
     JOIN offices offi ON o.id = offi."organizationId"
  ORDER BY o.id, offi.cd_office
WITH DATA;
