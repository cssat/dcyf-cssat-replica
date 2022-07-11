DROP MATERIALIZED VIEW IF EXISTS dcyf.visitation_referral_action_log;
CREATE MATERIALIZED VIEW IF NOT EXISTS dcyf.visitation_referral_action_log AS

WITH timeline AS (
	SELECT "ServiceReferralId" id_visitation_referral,
	NULL::int routed_from,
	"OrganizationId" id_organization,
	date dt_event,
	CASE WHEN "StageTypeId" = 7 THEN "createdAt"
	ELSE timestamp END AS ts_event,
	"StageTypeId" cd_event
	FROM dcyf.service_referral_timeline_stages
), emergent_orgs AS (
	SELECT DISTINCT destination_org_id
	FROM replica.referral_routes 
	WHERE routing_field_value LIKE 'Emergent%'
	AND routing_field_value NOT LIKE '%(Region 1)'
	AND routing_field_value NOT LIKE '%(Region 2)'
), routing_orgs AS (
	SELECT id 
	FROM replica."Organizations" 
	WHERE "deletedAt" IS NULL 
	AND "routingOrg"
	AND id NOT IN (SELECT destination_org_id FROM emergent_orgs)
), dtpr AS (
	SELECT id,
	lag_org routed_from,
	org_id id_organization,
	lag_updated_at::date dt_event,
	lag_updated_at ts_event,
	13 cd_event
	FROM (
		SELECT DISTINCT id,
		"organizationId" org_id,
		LAG("organizationId") OVER(PARTITION BY id ORDER BY "versionId") AS lag_org,
		LAG("updatedAt") OVER(PARTITION BY id ORDER BY "versionId") AS lag_updated_at,
		MIN(id) OVER(PARTITION BY id) AS min_id
		FROM replica."ServiceReferrals"
		WHERE "formVersion" = 'Ingested') sr
	WHERE org_id != lag_org
	AND org_id NOT IN (SELECT id FROM routing_orgs)
	AND lag_org IN (SELECT id FROM routing_orgs)
), union_log AS (
	SELECT * 
	FROM timeline
	UNION
	SELECT * 
	FROM dtpr
), action_log AS (
	SELECT 
	id_visitation_referral "ID_Visitation_Referral",
	CASE WHEN emergent_organization THEN 2 
	WHEN routing_organization THEN 1 
	ELSE 0 END AS "CD_Organization_Type",
	CASE WHEN emergent_organization THEN 'Emergent Organization'::varchar
	WHEN routing_organization THEN 'Routing Organization'::varchar 
	ELSE 'Sprout Provider'::varchar END AS "Organization_Type",
	organization_name "Organization_Name",
	id_organization "ID_Organization",
	dt_event "DT_Event",
	ts_event "TS_Event",
	CASE WHEN id_organization IN (SELECT destination_org_id FROM emergent_orgs) AND cd_event = 7 
	THEN 13 ELSE cd_event END AS "CD_Event",
	CASE WHEN cd_event = 13 OR (id_organization IN (SELECT destination_org_id FROM emergent_orgs) AND cd_event = 7) 
	THEN 'Queued: Referral placed in provider queue on:'::varchar
	ELSE tx_event::varchar END AS "TX_Event"
	FROM union_log t
	LEFT JOIN (SELECT id, 
			   name organization_name, 
			   "routingOrg" routing_organization
			   FROM dcyf.organizations) o
	ON t.id_organization = o.id
	LEFT JOIN (SELECT DISTINCT destination_org_id, 
			   TRUE emergent_organization 
			   FROM replica.referral_routes 
			   WHERE routing_field_value LIKE 'Emergent%') r
	ON t.id_organization = r.destination_org_id
	LEFT JOIN (SELECT id,
			   name || ' - ' || label tx_event
			   FROM dcyf.stage_types) s
	ON t.cd_event = s.id
)

SELECT *,
CASE WHEN "CD_Event" = 7 THEN 1
WHEN "CD_Event" = 13 THEN 2
WHEN "CD_Event" = 8 THEN 3
WHEN "CD_Event" = 12 THEN 4
WHEN "CD_Event" = 9 THEN 5
WHEN "CD_Event" = 10 THEN 6
WHEN "CD_Event" = 11 THEN 7
END AS "Order_Event",
now() "DT_View_Refreshed"
FROM action_log ORDER BY "ID_Visitation_Referral" DESC, "DT_Event", "Order_Event" 