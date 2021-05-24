
SELECT cron.schedule('refresh organization locations', '0 22 * * *', 'REFRESH MATERIALIZED VIEW curated."OrganizationLocations_mv"');

SELECT cron.schedule('refresh organizations', '0 22 * * *', 'REFRESH MATERIALIZED VIEW curated."Organizations_mv"');

SELECT cron.schedule('refresh dcyf referral compliment', '0 22 * * *', 'REFRESH MATERIALIZED VIEW curated."ServiceReferralIdsNonDCYF_mv"');

SELECT cron.schedule('refresh timeline stages', '0 22 * * *', 'REFRESH MATERIALIZED VIEW curated."ServiceReferralTimelineStages_mv"');

SELECT cron.schedule('refresh referrals', '0 22 * * *', 'REFRESH MATERIALIZED VIEW curated."ServiceReferrals_mv"');

SELECT cron.schedule('refresh stage types', '0 22 * * *', 'REFRESH MATERIALIZED VIEW curated."StageTypes_mv"');

SELECT cron.schedule('refresh uir action types', '0 22 * * *', 'REFRESH MATERIALIZED VIEW curated."UnusualIncidentActionTypes_mv"');

SELECT cron.schedule('refresh uir parties', '0 22 * * *', 'REFRESH MATERIALIZED VIEW curated."UnusualIncidentInvolvedParties_mv"');

SELECT cron.schedule('refresh uir party types', '0 22 * * *', 'REFRESH MATERIALIZED VIEW curated."UnusualIncidentInvolvedPartyTypes_mv"');

SELECT cron.schedule('refresh uir actions', '0 22 * * *', 'REFRESH MATERIALIZED VIEW curated."UnusualIncidentReportActions_mv"');

SELECT cron.schedule('refresh uir', '0 22 * * *', 'REFRESH MATERIALIZED VIEW curated."UnusualIncidentReports_mv"');

SELECT cron.schedule('refresh visit reports', '15 22 * * *', 'REFRESH MATERIALIZED VIEW curated."VisitReports_mv"');

UPDATE cron.job SET database = 'sprout_replica';