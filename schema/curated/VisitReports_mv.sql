CREATE MATERIALIZED VIEW IF NOT EXISTS curated."VisitReports_mv" AS

SELECT  
    vr."versionId",
    vr."serviceReferralId",
    vr."createdAt",
    vr."updatedAt",
    vr."state",
    vr."updatedById",
    vr."approvedById",
    vr."approvedAt",
    vr."cancellationType",
    vr."visitType",
    vr."caseNumber",
    vr."caseLastName",
    vr."caseFirstName",
    vr."socialWorkerLastName",
    vr."socialWorkerFirstName",
    vr."dcfsOffice",
    vr."visitLocationType",
    vr."visitLocationTitle",
    vr."visitLocationAddress",
    vr."visitLocationCity",
    vr."visitLocationState",
    vr."visitLocationCounty",
    vr."transportDistance",
    vr."transportTime",
    vr."waitTime",
    vr."comments",
    vr."visitAttendees",
    vr."furthestPage",
    vr."signature",
    vr."submittedAt",
    vr."id",
    vr."isCurrentVersion",
    vr."reportType",
    vr."actionsTaken",
    vr."explanationIfMissed",
    vr."deletedAt",
    vr."observations",
    vr."causedBy",
    vr."cause",
    vr."visitDescription",
    vr."numberOfChildren",
    vr."incidentTypeId",
    vr."incidentNarrative",
    vr."incidentResponses",
    vr."incidentComments",
    vr."isStriveEnabled",
    vr."submittedById",
    vr."activityFees",
    vr."levelOfSupervision",
    vr."importVersion",
    vr."formVersion",
    vr."otherTravelReimbursement",
    vr."additionalReimbursementComments",
    vr."activityReimbursement",
    vr."transportDetails",
    vr."waitTimes",
    vr."versionCreatedAt",
    vr."date",
    vr."endTime",
    vr."time",
    vr."virtual",
    vr."namedReimbursements"
FROM sprout."VisitReports" vr
WHERE vr."serviceReferralId" NOT IN (SELECT id FROM curated."ServiceReferralIdsNonDCYF_mv");

CREATE UNIQUE INDEX IF NOT EXISTS "VisitReports_mv_pkey" 
    ON curated."VisitReports_mv" USING btree ("versionId");
CREATE INDEX IF NOT EXISTS visit_reports_view_approved_by_date 
    ON curated."VisitReports_mv" USING btree (date) 
    WHERE (("deletedAt" IS NULL) 
        AND "isCurrentVersion" 
        AND ((state)::text = 'approved'::text));
CREATE INDEX IF NOT EXISTS visit_reports_view_cv_sr_id 
    ON curated."VisitReports_mv" USING btree ("serviceReferralId") 
    WHERE (("deletedAt" IS NULL) 
        AND "isCurrentVersion");
CREATE INDEX IF NOT EXISTS visit_reports_view_deleted_at 
    ON curated."VisitReports_mv" USING btree ("deletedAt");
CREATE INDEX IF NOT EXISTS visit_reports_view_deleted_at 
    ON curated."VisitReports_mv" USING btree ("createdAt");
CREATE INDEX IF NOT EXISTS visit_reports_view_id 
    ON curated."VisitReports_mv" USING btree (id);
CREATE INDEX IF NOT EXISTS visit_reports_view_is_current_version 
    ON curated."VisitReports_mv" USING btree ("isCurrentVersion");
CREATE INDEX IF NOT EXISTS visit_reports_view_service_referral_id 
    ON curated."VisitReports_mv" USING btree ("serviceReferralId");