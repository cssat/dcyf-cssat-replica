CREATE MATERIALIZED VIEW IF NOT EXISTS curated."ServiceReferralTimelineStages_mv" AS

SELECT 
    srt."id",
    srt."createdAt",
    srt."deletedAt",
    srt."updatedAt",
    srt."date",
    srt."OrganizationId",
    srt."ServiceReferralId",
    srt."UserId",
    srt."StageTypeId",
    srt."isEdit",
    srt."timestamp",
    srt."reason",
    srt."explanation" 
FROM sprout."ServiceReferralTimelineStages" srt
WHERE srt."ServiceReferralId" NOT IN (SELECT id FROM curated."ServiceReferralIdsNonDCYF_mv");

CREATE INDEX service_referral_timeline_stages_mv__service_referral_id 
    ON curated."ServiceReferralTimelineStages_mv" USING btree ("ServiceReferralId");

CREATE UNIQUE INDEX IF NOT EXISTS "ServiceReferralTimelineStages_mv_pkey" 
    ON curated."ServiceReferralTimelineStages_mv" USING btree ("id");
