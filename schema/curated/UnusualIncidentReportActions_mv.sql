CREATE MATERIALIZED VIEW IF NOT EXISTS curated."UnusualIncidentReportActions_mv" AS

SELECT  
    uia."id",
    uia."unusualIncidentReportId",
    uia."actionTakenId",
    uia."otherActionTaken",
    uia."createdAt",
    uia."updatedAt",
    uia."deletedAt",
    now() "viewRefreshedAt"
FROM sprout."UnusualIncidentReportActions" uia
    JOIN curated."UnusualIncidentReports_mv" uir
        ON uia."unusualIncidentReportId" = uir.id;

CREATE UNIQUE INDEX IF NOT EXISTS "UnusualIncidentReportActions_mv_pkey" 
    ON curated."UnusualIncidentReportActions_mv" USING btree ("id");