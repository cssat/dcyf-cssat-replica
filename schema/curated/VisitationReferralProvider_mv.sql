DROP MATERIALIZED VIEW IF EXISTS dcyf.visitation_referral_provider;
CREATE MATERIALIZED VIEW IF NOT EXISTS dcyf.visitation_referral_provider AS

WITH org AS (
	SELECT o.id,
		o.name
	FROM replica."Organizations" AS o
    JOIN replica."OrganizationContracts" AS oc 
        ON o.id = oc."contractedOrganizationId"
        AND oc."contractOwnerId" = 21
), sr AS (
	SELECT id
	FROM replica."ServiceReferrals" sr
	WHERE sr."organizationId" IN (SELECT id FROM org)
), sr_non_dcyf AS (
	SELECT DISTINCT id
	FROM replica."ServiceReferrals" sr
	WHERE id NOT IN (SELECT DISTINCT id FROM sr)
), routing_orgs AS (
	SELECT id 
	FROM replica."Organizations" 
	WHERE "deletedAt" IS NULL 
	AND "routingOrg"
), dtpr AS (
	SELECT id,
	org_id routed_to,
	MIN(lag_updated_at) dt_provider_received
	FROM (
		SELECT DISTINCT id,
		"organizationId" org_id,
		LAG("organizationId") OVER(PARTITION BY id ORDER BY "versionId") AS lag_org,
		LAG("updatedAt") OVER(PARTITION BY id ORDER BY "versionId") AS lag_updated_at
		FROM replica."ServiceReferrals"
		WHERE "formVersion" = 'Ingested') sr
	WHERE org_id != lag_org
	AND org_id NOT IN (SELECT id FROM routing_orgs)
	AND lag_org IN (SELECT id FROM routing_orgs)
	GROUP BY id, org_id
), srts AS (
	SELECT "ServiceReferralId" AS id,
	"OrganizationId" AS org_id,
	MAX(CASE WHEN "StageTypeId" = 8 THEN "timestamp" END) AS dt_provider_accepted, 
	MAX(CASE WHEN "StageTypeId" = 12 THEN "timestamp" END) AS dt_provider_rejected
	FROM replica."ServiceReferralTimelineStages"
	GROUP BY  "ServiceReferralId", "OrganizationId"
), vc_dat AS (
	SELECT "ServiceReferralId" AS id,
	"OrganizationId" AS org_id,
	"UserId" AS id_visit_coodinator,
	CONCAT("lastName", ', ', "firstName") AS visit_coodinator_name
	FROM(SELECT "ServiceReferralId",
		 "UserId",
		 "createdAt",
		 "OrganizationId",
		 MAX("createdAt") OVER (PARTITION BY "ServiceReferralId", "OrganizationId") AS dt_referral_received
		 FROM replica."ServiceReferralTimelineStages"
		 WHERE "StageTypeId" = 7) as dat
	JOIN replica."Users" AS u
	ON dat."UserId" = u.id
	WHERE dat."createdAt" = dt_referral_received
), service_referrals AS (
	SELECT * 
	FROM (
		SELECT id, 
		"visitPlanId",
		"routingOrganizationId",
		"organizationId",
		"createdAt",
		"startDate",
		"endDate",
		"updatedAt",
		MAX("updatedAt") OVER (PARTITION BY id, "organizationId") AS max_updated_at,
		MAX("deletedAt") OVER (PARTITION BY id) AS any_deleted_at
		FROM replica."ServiceReferrals"
		WHERE "formVersion" = 'Ingested'
		AND id NOT IN (SELECT id FROM sr_non_dcyf)) sr
	WHERE "updatedAt" = max_updated_at
	AND any_deleted_at IS NULL
), visitation_referral_provider_tbl AS (
SELECT sr.id AS id_visitation_referral, 
	"visitPlanId" AS id_visit_plan,
	id_visit_coodinator, 
	"routingOrganizationId" AS id_routing_organization,
	ro.name AS routing_organization_name, 
	visit_coodinator_name,
	"organizationId" AS id_provider_sprout,
	po.name AS provider_name_sprout,
	regexp_replace(split_part(po."famlinkId", '/', 1), '\D', '', 'g') AS id_pvdr_org_dcyf,
	regexp_replace(split_part(po."famlinkId", '/', 2), '\D', '', 'g') AS id_pvdr_org_fin,
	sr."createdAt" dt_referral_received,
	"startDate" AS dt_start,
	"endDate" AS dt_end,
	dt_provider_received,
	dt_provider_accepted,
	dt_provider_rejected,
	CASE WHEN (dt_provider_accepted IS NULL AND dt_provider_rejected IS NOT NULL) 
	OR dt_provider_rejected > dt_provider_accepted THEN 2
	WHEN (dt_provider_accepted IS NOT NULL AND dt_provider_rejected IS NULL)
	OR dt_provider_accepted > dt_provider_rejected THEN 1
	END AS cd_provider_decision
FROM service_referrals AS sr
JOIN replica."Organizations" AS ro
	ON sr."routingOrganizationId" = ro.id
JOIN replica."Organizations" AS po
 	ON sr."organizationId" = po.id
LEFT JOIN srts
	ON sr.id = srts.id
	AND sr."organizationId" = srts.org_id
LEFT JOIN vc_dat AS vd
	ON sr.id = vd.id
	AND sr."routingOrganizationId" = vd.org_id
LEFT JOIN dtpr
	ON sr.id = dtpr.id
	AND sr."organizationId" = dtpr.routed_to)
SELECT 
concat_ws('_'::varchar, id_visitation_referral, id_provider_sprout)::varchar "ID_Visitation_Referral_Provider",
id_visitation_referral "ID_Visitation_Referral", 
id_visit_plan::int "ID_Visit_Plan",
id_visit_coodinator "ID_Visit_Coordinator", 
id_routing_organization "ID_Routing_Organization",
routing_organization_name "Routing_Organization_Name", 
visit_coodinator_name::varchar "Visit_Coordinator_Name",
id_provider_sprout "ID_Provider_Sprout",
provider_name_sprout "Provider_Name_Sprout",
CASE WHEN id_pvdr_org_dcyf = '' THEN NULL ELSE id_pvdr_org_dcyf::int END AS "ID_Provider_Org_DCYF",
CASE WHEN id_pvdr_org_fin = '' THEN NULL ELSE id_pvdr_org_fin::int END AS "ID_Provider_Org_FIN",
dt_referral_received "DT_Referral_Received",
dt_start "DT_Start",
dt_end "DT_End",
dt_provider_received "DT_Provider_Received",
CASE WHEN cd_provider_decision = 1 THEN dt_provider_accepted
WHEN cd_provider_decision = 2 THEN dt_provider_rejected
END AS "DT_Provider_Decision",
cd_provider_decision "CD_Provider_Decision",
CASE WHEN cd_provider_decision = 1 THEN 'Accepted'::varchar
WHEN cd_provider_decision = 2 THEN 'Rejected'::varchar 
END AS "Provider_Decision",
now() "DT_View_Refreshed"
FROM visitation_referral_provider_tbl;