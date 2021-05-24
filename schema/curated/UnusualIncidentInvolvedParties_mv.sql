CREATE MATERIALIZED VIEW IF NOT EXISTS curated."UnusualIncidentInvolvedParties_mv" AS

SELECT  
    uip."id",
    uip."firstName",
    uip."lastName",
    uip."birthDate",
    uip."unusualIncidentReportId",
    uip."involvedPartyTypeId",
    uip."otherInvolvedPartyType",
    uip."createdAt",
    uip."updatedAt",
    uip."deletedAt",
    now() "viewRefreshedAt"
FROM sprout."UnusualIncidentInvolvedParties" uip
    JOIN curated."UnusualIncidentReports_mv" uir
        ON uip."unusualIncidentReportId" = uir.id;

CREATE UNIQUE INDEX IF NOT EXISTS "UnusualIncidentInvolvedParties_mv_pkey" 
    ON curated."UnusualIncidentInvolvedParties_mv" USING btree ("id");