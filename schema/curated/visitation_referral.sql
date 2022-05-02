-- View: dcyf.visitation_referral

DROP MATERIALIZED VIEW IF EXISTS dcyf.visitation_referral;

CREATE MATERIALIZED VIEW IF NOT EXISTS dcyf.visitation_referral
TABLESPACE pg_default AS
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
		MAX(CASE WHEN "StageTypeId" = 12 THEN "timestamp" END) AS dt_provider_rejected,
		MIN(CASE WHEN "StageTypeId" = 9 THEN "date" END) AS dt_first_visit_scheduling_confirmation,
		MAX(CASE WHEN "StageTypeId" = 9 THEN "date" END) AS dt_final_visit_scheduling_confirmation,
		MIN(CASE WHEN "StageTypeId" = 10 THEN "date" END) AS dt_first_visit_scheduled,
		MIN(CASE WHEN "StageTypeId" = 11 THEN "date" END) AS dt_referral_provider_resolved,
		SUM(CASE WHEN "StageTypeId" = 10 THEN 1 END) AS schedule_attempt_count
	FROM replica."ServiceReferralTimelineStages"
	GROUP BY  "ServiceReferralId", "OrganizationId"
), fl_data AS (
	SELECT id, 
		MAX(CASE WHEN safety_issues->>'angerOutbursts' = 'true' THEN 1 END) AS fl_safety_issue_anger_outburst,
		MAX(CASE WHEN safety_issues->>'inappropriateConversation' = 'true' THEN 1 END) AS fl_safety_issue_inappropriate_conversation,
		MAX(CASE WHEN safety_issues->>'restrainingOrder' = 'true' THEN 1 END) AS fl_safety_issue_no_contact_order, 
		MAX(CASE WHEN safety_issues->>'domesticViolence' = 'true' THEN 1 END) AS fl_safety_issue_dv,
		COUNT(DISTINCT child_details->>'childFamlinkPersonID') AS child_count,
		COUNT(DISTINCT parent_guardian_details->>'parentGuardianId') AS parent_count
	FROM replica."ServiceReferrals", 
		json_array_elements("safetyIssues") safety_issues,
		json_array_elements("childDetails") child_details,
 		json_array_elements("parentGuardianDetails") parent_guardian_details
	WHERE "deletedAt" IS NULL
		AND "isCurrentVersion"
		AND "formVersion" = 'Ingested'
	GROUP BY id
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
), first_visit AS (
	SELECT "serviceReferralId",
		MIN(date) AS dt_first_visit_occurred
	FROM replica."VisitReports"
	WHERE "deletedAt" IS NULL
		AND "isCurrentVersion"
		AND state = 'approved'
		AND "reportType" IN ('Parent', 'Sibling')
	GROUP BY "serviceReferralId"
), report_count AS (
	SELECT "serviceReferralId",
		COUNT(*) AS visit_report_count
	FROM replica."VisitReports"
	WHERE "deletedAt" IS NULL
		AND "isCurrentVersion"
		AND state = 'approved'
	GROUP BY "serviceReferralId"
), attended_report_count AS (
	SELECT "serviceReferralId",
		COUNT(*) AS visit_report_attended_count
	FROM replica."VisitReports"
	WHERE "deletedAt" IS NULL
		AND "isCurrentVersion"
		AND state = 'approved'
		AND "reportType" IN ('Parent', 'Sibling')
	GROUP BY "serviceReferralId"
), service_referrals AS (
	SELECT * 
	FROM (
		SELECT *,
		MAX("deletedAt") OVER (PARTITION BY id) AS any_deleted_at
		FROM replica."ServiceReferrals") sr
	WHERE any_deleted_at IS NULL
), visitation_referral_tbl AS (
SELECT sr.id AS id_visitation_referral, 
	"visitPlanId" AS id_visit_plan,
	"caseNumber" AS id_case,
	sr."regionId" AS cd_region,
	CASE WHEN sr."regionId" IS NULL THEN NULL
		ELSE CONCAT('Region ', sr."regionId") 
		END AS region,
	regexp_replace("dshsOffice", '\D', '', 'g')::INT AS cd_office,	
	"dshsOffice" AS office,
	"socialWorkerId" AS id_worker,
	CONCAT("socialWorkerLastName", ', ', "socialWorkerFirstName") AS worker_name,
	id_visit_coodinator, 
	"routingOrganizationId" AS id_routing_organization,
	ro.name AS routing_organization_name, 
	visit_coodinator_name,
	"organizationId" AS id_provider_sprout,
	po.name AS provider_name_sprout,
	regexp_replace(split_part(po."famlinkId", '/', 1), '\D', '', 'g') AS id_pvdr_org_dcyf,
	regexp_replace(split_part(po."famlinkId", '/', 2), '\D', '', 'g') AS id_pvdr_org_fin,
	CASE WHEN "referralReason" = 'Initial' THEN 1
		WHEN "referralReason" LIKE '%Re-referral - Parent%' THEN 2
		WHEN "referralReason" = 'Re-referral - Provider dropped' THEN 3
		WHEN "referralReason" = 'Update - Changes to visit location, frequency, duration or level of supervision' THEN 4
		WHEN "referralReason" = 'Reauthorization - All supervised visits every 6 months' THEN 5
		WHEN "referralReason" LIKE '%Emergent 72%' THEN 6
		END AS cd_referral_reason,
	"referralReason" AS referral_reason,
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
	END AS cd_provider_decision,
	"intakeDateCompleted" AS dt_intake, 
	"intakeTotalTime" AS intake_time, 
	schedule_attempt_count, 
	dt_final_visit_scheduling_confirmation,
	dt_first_visit_scheduling_confirmation, 
	dt_first_visit_scheduled,
	dt_first_visit_occurred,
	dt_referral_provider_resolved,
	CASE WHEN dt_referral_provider_resolved < "endDate" THEN dt_referral_provider_resolved
		WHEN dt_referral_provider_resolved > "endDate" THEN "endDate"
		ELSE COALESCE(dt_referral_provider_resolved, "endDate") 
		END AS dt_referral_closed,
	parent_count,
	child_count,
	CASE WHEN "visitTransportationType" IN ('Transportation Only', 'With Transportation') THEN 1
		ELSE 0
		END AS fl_transport_required,
	fl_safety_issue_anger_outburst, 
	fl_safety_issue_inappropriate_conversation, 
	fl_safety_issue_no_contact_order, 
	fl_safety_issue_dv,
	CASE WHEN "levelOfSupervision" = 'Unsupervised' THEN 1
		WHEN "levelOfSupervision" = 'Monitored' THEN 2
		WHEN "levelOfSupervision" = 'Unsupervised' THEN 3
		END AS cd_supervision_level,
	"levelOfSupervision" AS supervision_level,
	"visitFrequency" AS visit_frequency, 
	"hoursPerVisit" AS visit_duration_hours,
	CASE WHEN "serviceType" = 'Parent / Child' THEN 1
		WHEN "serviceType" = 'Sibling' THEN 2
		END AS cd_visitation_referral_type,
	"serviceType" AS visitation_referral_type,
	CASE WHEN "visitTransportationType" = 'With Transportation' THEN 1
		WHEN "visitTransportationType" = 'Without Transportation' THEN 2
		WHEN "visitTransportationType" = 'Transportation Only' THEN 3
		END AS cd_transportation_type,
	"visitTransportationType" AS transportation_type,
	visit_report_count,
	visit_report_attended_count
FROM service_referrals AS sr
JOIN replica."Organizations" AS ro
	ON sr."routingOrganizationId" = ro.id
JOIN replica."Organizations" AS po
 	ON sr."organizationId" = po.id
LEFT JOIN srts
	ON sr.id = srts.id
	AND sr."organizationId" = srts.org_id
LEFT JOIN fl_data AS fd
	ON sr.id = fd.id
LEFT JOIN vc_dat AS vd
	ON sr.id = vd.id
	AND sr."routingOrganizationId" = vd.org_id
LEFT JOIN dtpr
	ON sr.id = dtpr.id
	AND sr."organizationId" = dtpr.routed_to
LEFT JOIN first_visit AS fv
	ON sr.id = fv."serviceReferralId"
LEFT JOIN report_count AS rc
	ON sr.id = rc."serviceReferralId"
LEFT JOIN attended_report_count AS arc
	ON sr.id = arc."serviceReferralId"
WHERE sr."deletedAt" IS NULL
	AND sr."isCurrentVersion"
	AND "formVersion" = 'Ingested'
	AND sr.id NOT IN (SELECT id FROM sr_non_dcyf))
SELECT 
id_visitation_referral "ID_Visitation_Referral", 
id_visit_plan::int "ID_Visit_Plan",
id_case::int "ID_Case",
cd_region::varchar "CD_Region",
region "Region",
cd_office "CD_Office",	
office "Office",
id_worker "ID_Worker",
worker_name::varchar "Worker_Name",
id_visit_coodinator "ID_Visit_Coordinator", 
id_routing_organization "ID_Routing_Organization",
routing_organization_name "Routing_Organization_Name", 
visit_coodinator_name::varchar "Visit_Coordinator_Name",
id_provider_sprout "ID_Provider_Sprout",
provider_name_sprout "Provider_Name_Sprout",
CASE WHEN id_pvdr_org_dcyf = '' THEN NULL ELSE id_pvdr_org_dcyf::int END AS "ID_Provider_Org_DCYF",
CASE WHEN id_pvdr_org_fin = '' THEN NULL ELSE id_pvdr_org_fin::int END AS "ID_Provider_Org_FIN",
cd_referral_reason "CD_Referral_Reason",
referral_reason "Referral_Reason",
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
dt_intake "DT_Intake", 
intake_time::real "Intake_Time", 
schedule_attempt_count::int "Schedule_Attempt_Count", 
dt_final_visit_scheduling_confirmation "DT_Final_Visit_Scheduling_Confirmation",
dt_first_visit_scheduling_confirmation "DT_First_Visit_Scheduling_Confirmation", 
dt_first_visit_scheduled "DT_First_Visit_Scheduled",
dt_first_visit_occurred "DT_First_Visit_Occurred",
dt_referral_provider_resolved "DT_Referral_Provider_Resolved",
dt_referral_closed "DT_Referral_Closed",
parent_count::int "Parent_Count",
child_count::int "Child_Count",
CASE WHEN cd_provider_decision = 1 THEN 1::smallint 
ELSE 0::smallint END AS "FL_Accepted",
fl_transport_required::smallint "FL_Transport_Required",
fl_safety_issue_anger_outburst::smallint "FL_Safety_Issue_Anger_Outburst", 
fl_safety_issue_inappropriate_conversation::smallint "FL_Safety_Issue_Inappropriate_Conversation", 
fl_safety_issue_no_contact_order::smallint "FL_Safety_Issue_No_Contact_Order", 
fl_safety_issue_dv::smallint "FL_Safety_Issue_DV",
cd_supervision_level "CD_Supervision_Level",
supervision_level "Supervision_Level",
visit_frequency::smallint "Visit_Frequency", 
visit_duration_hours::real "Visit_Duration_Hours",
cd_visitation_referral_type "CD_Visitation_Referral_Type",
visitation_referral_type "Visitation_Referral_Type",
cd_transportation_type "CD_Transportation_Type",
transportation_type "Transportation_Type",
visit_report_count::int "Visit_Report_Count",
visit_report_attended_count::int "Visit_Report_Attended_Count"
FROM visitation_referral_tbl 
WITH DATA;

ALTER TABLE IF EXISTS dcyf.visitation_referral
    OWNER TO aptible;

GRANT ALL ON TABLE dcyf.visitation_referral TO aptible;
GRANT SELECT ON TABLE dcyf.visitation_referral TO dcyf_users;