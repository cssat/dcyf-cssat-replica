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
	/* 
	use the id and org id (visit coord) to get dt_provider_received from this date 
	when did it go to vc id to a provider id REMEMBER org id may not be provider
	NEED first row provider with a provider and previous row was a VC (maybe routing org)
	-- WHICH PROVIDER DO THEY WANT? might not be able to get the data
	*/
	SELECT "organizationId",
		id,
		MIN("createdAt") AS dt_provider_received
	FROM staging."ServiceReferrals"
	WHERE id = 58275
	GROUP BY "organizationId",
		id
HAVING MAX(CASE WHEN "deletedAt" IS NOT NULL THEN 1 ELSE 0 END) = 0
), srts AS (
	SELECT "ServiceReferralId" AS id,
		MIN(CASE WHEN "StageTypeId" = 7 THEN "createdAt" END) AS dt_referral_received,
		MIN(CASE WHEN "StageTypeId" = 8 THEN "timestamp" END) AS dt_provider_decision, -- with stage 12 reject/accept max timestamp for 8 or 12
		MIN(CASE WHEN "StageTypeId" = 9 THEN "date" END) AS dt_first_visit_scheduling_confirmation,
		MAX(CASE WHEN "StageTypeId" = 9 THEN "date" END) AS dt_final_visit_scheduling_confirmation,
		MIN(CASE WHEN "StageTypeId" = 10 THEN "date" END) AS dt_first_visit_scheduled,
		MIN(CASE WHEN "StageTypeId" = 11 THEN "date" END) AS dt_referral_provider_resolved,
		MAX(CASE WHEN "StageTypeId" = 12 THEN 1 END) AS rejected,
		SUM(CASE WHEN "StageTypeId" = 10 THEN 1 END) AS schedule_attempt_count
	FROM staging."ServiceReferralTimelineStages"
	GROUP BY  "ServiceReferralId"
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
	NULL id_visit_coodinator, -- look in timeline stages for user id transition from visit coordinator to provider
	"routingOrganizationId" AS id_routing_organization,
	ro.name AS routing_organization_name, 
	NULL AS visit_coodinator_name, -- look in timeline stages for user id transition from visit coordinator to provider
	"organizationId" AS id_provider_sprout,
	po.name AS provider_name_sprout,
	regexp_replace(po."famlinkId", '\D', '', 'g') AS id_pvdr_org,
	NULL AS cd_referral_reason, -- case statement off of "referralReason"
	"referralReason" AS referral_reason,
	dt_referral_received, -- check
	"startDate" AS dt_start, -- check
	"endDate" AS dt_end, -- check
	NULL AS dt_provider_received, -- need to get first record of provider with sr.id 
	dt_provider_decision,
	"intakeDateCompleted" AS dt_intake, 
	"intakeTotalTime" AS intake_time, 
	schedule_attempt_count, -- not sure if logic is correct
	dt_final_visit_scheduling_confirmation,
	dt_first_visit_scheduling_confirmation, 
	dt_first_visit_scheduled,
	NULL AS dt_first_visit_occurred, -- first attended visit "reportType" = parent or sibling do they care about sibling visits?
	dt_referral_provider_resolved,
	NULL AS dt_referral_closed, -- min date between dt_referral_provider_resolved and dt_end
	NULL AS Parent_Count, -- parentGaurdian? not sure
	NULL AS Child_Count, -- childDetials?
	NULL AS CD_Provider_Decision, -- get from provider decision
	CASE WHEN rejected IS NULL THEN 'Accepted'
		ELSE 'Rejected' 
		END AS provider_decision,
	CASE WHEN rejected IS NULL THEN 1 
		ELSE 0 
		END AS fl_accepted,
	CASE WHEN "visitTransportationType" IN ('Transportation Only', 'With Transportation') THEN 1
		ELSE 0
		END AS FL_Transport_Required,
	NULL AS FL_Safety_Issue_Anger_Outburst, -- safetyIssues
	NULL AS FL_Safety_Issue_Inappropriate_Conversation, -- safetyIssues
	NULL AS FL_Safety_Issue_No_Contact_Order, -- safetyIssues
	NULL AS FL_Safety_Issue_DV, -- safetyIssues
	CASE WHEN "levelOfSupervision" = 'Unsupervised' THEN 1
		WHEN "levelOfSupervision" = 'Monitored' THEN 2
		WHEN "levelOfSupervision" = 'Unsupervised' THEN 3
		END AS CD_Supervision_Level,
	"levelOfSupervision" AS supervision_level,
	"visitFrequency" AS Visit_Frequency, --do we need to consider visit frequency unit 
	"hoursPerVisit" AS visit_duration_hours,
	NULL AS CD_Visitation_Referral_Type,
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
WHERE sr."deletedAt" IS NULL
	AND sr."isCurrentVersion"
	AND sr.id NOT IN (SELECT id FROM sr_non_dcyf)