WITH org AS (
	SELECT o.id,
		o.name
	FROM staging."Organizations" AS o
    JOIN staging."OrganizationContracts" AS oc 
        ON o.id = oc."contractedOrganizationId"
        AND oc."contractOwnerId" = 21
), sr AS (
	SELECT id
	FROM staging."ServiceReferrals" sr
	WHERE sr."organizationId" IN (SELECT id FROM org)
), sr_non_dcyf AS (
	SELECT DISTINCT id
	FROM staging."ServiceReferrals" sr
	WHERE id NOT IN (SELECT DISTINCT id FROM sr)
), dtpr AS (
	WITH routing_orgs as (
		SELECT id 
		FROM staging."Organizations" 
		WHERE "deletedAt" IS NULL 
			AND "routingOrg"
	) 
	SELECT DISTINCT id, 
		"organizationId" AS routed_to, 
		time 
	FROM staging."ServiceReferrals" AS foo, 
	LATERAL (SELECT MAX("updatedAt") AS time 
		 	FROM staging."ServiceReferrals" AS bar 
		 	WHERE bar.id = foo.id 
		 		AND bar."versionId" < foo."versionId" 
		 		AND bar."organizationId" != foo."organizationId" 
		 		AND bar."organizationId" IN (SELECT id FROM routing_orgs) GROUP BY id) AS baz 
	WHERE "formVersion" = 'Ingested';
srts AS (
	SELECT "ServiceReferralId" AS id,
		MAX(CASE WHEN "StageTypeId" = 7 THEN "createdAt" END) AS dt_referral_received,
		MAX(CASE WHEN "StageTypeId" = 8 THEN "timestamp" 
			WHEN "StageTypeId" = 12 THEN "timestamp" 
			END) AS dt_provider_decision, 
		MIN(CASE WHEN "StageTypeId" = 9 THEN "date" END) AS dt_first_visit_scheduling_confirmation,
		MAX(CASE WHEN "StageTypeId" = 9 THEN "date" END) AS dt_final_visit_scheduling_confirmation,
		MIN(CASE WHEN "StageTypeId" = 10 THEN "date" END) AS dt_first_visit_scheduled,
		MIN(CASE WHEN "StageTypeId" = 11 THEN "date" END) AS dt_referral_provider_resolved,
		MAX(CASE WHEN "StageTypeId" = 12 THEN 1 END) AS rejected,
		SUM(CASE WHEN "StageTypeId" = 10 THEN 1 END) AS schedule_attempt_count
	FROM staging."ServiceReferralTimelineStages"
	GROUP BY  "ServiceReferralId"
), fl_data AS (
	SELECT id, 
		MAX(CASE WHEN safety_issues->>'angerOutbursts' = 'true' THEN 1 END) AS fl_safety_issue_anger_outburst,
		MAX(CASE WHEN safety_issues->>'inappropriateConversation' = 'true' THEN 1 END) AS fl_safety_issue_inappropriate_conversation,
		MAX(CASE WHEN safety_issues->>'restrainingOrder' = 'true' THEN 1 END) AS fl_safety_issue_no_contact_order, 
		MAX(CASE WHEN safety_issues->>'domesticViolence' = 'true' THEN 1 END) AS fl_safety_issue_dv,
		COUNT(DISTINCT child_details->>'childFamlinkPersonID') AS child_count,
		COUNT(DISTINCT parent_guardian_details->>'parentGuardianId') AS parent_count
	FROM staging."ServiceReferrals", 
		json_array_elements("safetyIssues") safety_issues,
		json_array_elements("childDetails") child_details,
 		json_array_elements("parentGuardianDetails") parent_guardian_details
	WHERE "deletedAt" IS NULL
		AND "isCurrentVersion"
		AND "formVersion" = 'Ingested'
	GROUP BY id
), vc_dat AS (
	SELECT "ServiceReferralId" AS id,
		"UserId" AS id_visit_coodinator,
		CONCAT("lastName", ', ', "firstName") AS visit_coodinator_name
	FROM(SELECT "ServiceReferralId",
	 		"UserId",
			"createdAt",
			MAX("createdAt") OVER (PARTITION BY "ServiceReferralId") AS dt_referral_received
		FROM staging."ServiceReferralTimelineStages"
		WHERE "StageTypeId" = 7 
		GROUP BY "UserId", 
			"ServiceReferralId",
			"createdAt") as dat
	JOIN staging."Users" AS u
		ON dat."UserId" = u.id
	WHERE dat."createdAt" = dt_referral_received
), first_visit AS (
	SELECT "serviceReferralId",
		MIN(date + time) AS dt_first_visit_occurred
	FROM staging."VisitReports"
	WHERE "deletedAt" IS NULL
		AND "isCurrentVersion"
		AND "cancellationType" IS NULL
	GROUP BY "serviceReferralId"
)
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
	regexp_replace(po."famlinkId", '\D', '', 'g') AS id_pvdr_org,
	CASE WHEN "referralReason" = 'Initial' THEN 1
		WHEN "referralReason" LIKE '%Re-referral - Parent%' THEN 2
		WHEN "referralReason" = 'Re-referral - Provider dropped' THEN 3
		WHEN "referralReason" = 'Update - Changes to visit location, frequency, duration or level of supervision' THEN 4
		WHEN "referralReason" = 'Reauthorization - All supervised visits every 6 months' THEN 5
		WHEN "referralReason" LIKE '%Emergent 72%' THEN 6
		END AS cd_referral_reason,
	"referralReason" AS referral_reason,
	dt_referral_received,
	"startDate" AS dt_start,
	"endDate" AS dt_end,
	NULL AS dt_provider_received, -- not sure we can provide this in any accurate way
	dt_provider_decision,
	"intakeDateCompleted" AS dt_intake, 
	"intakeTotalTime" AS intake_time, 
	schedule_attempt_count, 
	dt_final_visit_scheduling_confirmation,
	dt_first_visit_scheduling_confirmation, 
	dt_first_visit_scheduled,
	NULL AS dt_first_visit_occurred, -- first attended visit "reportType" = parent or sibling do they care about sibling visits?
	dt_referral_provider_resolved,
	CASE WHEN dt_referral_provider_resolved < "endDate" THEN dt_referral_provider_resolved
		WHEN dt_referral_provider_resolved > "endDate" THEN "endDate"
		ELSE COALESCE(dt_referral_provider_resolved, "endDate") 
		END AS dt_referral_closed,
	parent_count,
	child_count,
	CASE WHEN rejected IS NULL THEN 1
		ELSE 2 
		END AS cd_provider_decision, 
	CASE WHEN rejected IS NULL THEN 'Accepted'
		ELSE 'Rejected' 
		END AS provider_decision,
	CASE WHEN rejected IS NULL THEN 1 
		ELSE 0 
		END AS fl_accepted,
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
	"visitTransportationType" AS transportation_type
FROM staging."ServiceReferrals" AS sr
JOIN staging."Organizations" AS ro
	ON sr."routingOrganizationId" = ro.id
JOIN staging."Organizations" AS po
 	ON sr."organizationId" = po.id
LEFT JOIN srts
	ON sr.id = srts.id
LEFT JOIN fl_data AS fd
	ON sr.id = fd.id
LEFT JOIN vc_dat AS vd
	ON sr.id = vd.id
WHERE sr."deletedAt" IS NULL
	AND sr."isCurrentVersion"
	AND "formVersion" = 'Ingested'
	AND sr.id NOT IN (SELECT id FROM sr_non_dcyf)