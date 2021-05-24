CREATE MATERIALIZED VIEW IF NOT EXISTS curated."ServiceReferralIdsNonDCYF_mv" AS

SELECT DISTINCT 
    id 
FROM sprout."ServiceReferrals"
WHERE 
    id 
NOT IN (
SELECT DISTINCT 
    id 
FROM curated."ServiceReferrals_mv"
);

CREATE UNIQUE INDEX IF NOT EXISTS "ServiceReferralIdsNonDCYF_mv_pkey" 
    ON curated."ServiceReferralIdsNonDCYF_mv" USING btree ("id");
