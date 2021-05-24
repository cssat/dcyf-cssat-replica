CREATE MATERIALIZED VIEW IF NOT EXISTS curated."UnusualIncidentReports_mv" AS

SELECT  
    uir."id",
    uir."incidentNarrative",
    uir."serviceReferralId",
    uir."state",
    uir."createdAt",
    uir."updatedAt",
    uir."deletedAt",
    uir."caseNumber",
    uir."approvedAt",
    uir."approvedById",
    uir."date",
    uir."notificationDate",
    uir."notificationTime",
    uir."staffNotifiedPhone",
    uir."staffNotifiedEmail",
    uir."staffNotifiedByMeans",
    uir."submittedById",
    uir."submittedAt",
    uir."furthestPage",
    uir."staffNotifiedLastName",
    uir."staffNotifiedFirstName",
    uir."importVersion",
    now() "viewRefreshedAt"
FROM sprout."UnusualIncidentReports" uir
WHERE uir."serviceReferralId" NOT IN (SELECT id FROM curated."ServiceReferralIdsNonDCYF_mv");


CREATE UNIQUE INDEX IF NOT EXISTS "UnusualIncidentReports_mv_pkey" 
    ON curated."UnusualIncidentReports_mv" USING btree ("id");
