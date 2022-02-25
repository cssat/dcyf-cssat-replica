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
), srts AS (
	SELECT "ServiceReferralId" AS id,
		MIN(CASE WHEN "StageTypeId" = 7 THEN "date" END) AS dt_referral_received,
		MIN(CASE WHEN "StageTypeId" = 8 THEN "date" END) AS dt_provider_decision,
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
	NULL id_visit_coodinator,
	"routingOrganizationId" AS id_routing_organization,
	ro.name AS routing_organization_name,
	NULL AS visit_coodinator_name,
	"organizationId" AS id_provider_sprout,
	po.name AS provider_name_sprout,
	regexp_replace(po."famlinkId", '\D', '', 'g') AS id_pvdr_org,
	NULL AS cd_referral_reason,
	"referralReason" AS referral_reason,
	dt_referral_received, -- check
	"startDate" AS dt_start, -- check
	"endDate" AS dt_end, -- check
	NULL AS dt_provider_received, --check
	dt_provider_decision,
	NULL AS dt_intake, -- not sure what this is
	NULL AS intake_time, -- not sure what this is
	schedule_attempt_count, -- not sure if logic is correct
	dt_final_visit_scheduling_confirmation, -- not sure if logic is correct
	dt_first_visit_scheduling_confirmation, -- not sure if logic is correct
	dt_first_visit_scheduled,
	NULL AS dt_first_visit_occurred,
	dt_referral_provider_resolved,
	NULL AS dt_referral_closed,
	NULL AS Parent_Count,
	NULL AS Child_Count,
	NULL AS CD_Provider_Decision,
	CASE WHEN rejected IS NULL THEN 'Accepted'
		ELSE 'Rejected' 
		END AS provider_decision,
	CASE WHEN rejected IS NULL THEN 1 
		ELSE 0 
		END AS fl_accepted,
	CASE WHEN "visitTransportationType" IN ('Transportation Only', 'With Transportation') THEN 1
		ELSE 0
		END AS FL_Transport_Required,
	NULL AS FL_Safety_Issue_Anger_Outburst,
	NULL AS FL_Safety_Issue_Inappropriate_Conversation,
	NULL AS FL_Safety_Issue_No_Contact_Order,
	NULL AS FL_Safety_Issue_DV,
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