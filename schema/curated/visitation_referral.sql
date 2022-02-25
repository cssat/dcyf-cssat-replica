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
		MAX(CASE WHEN "StageTypeId" = 9 THEN "date" END) AS dt_final_visit_scheduling_confirmation,
		MIN(CASE WHEN "StageTypeId" = 10 THEN "date" END) AS dt_first_visit_scheduled,
		MIN(CASE WHEN "StageTypeId" = 11 THEN "date" END) AS dt_referral_provider_resolved,
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
	dt_first_visit_scheduled,
	NULL AS dt_first_visit_occurred,
	dt_referral_provider_resolved,
	NULL AS dt_referral_closed
	/*
	Parent_Count,
	Child_Count,
	CD_Provider_Decision,
	Provider_Decision,
	FL_Accepted,
	FL_Transport_Required,
	FL_Safety_Issue_Anger_Outburst,
	FL_Safety_Issue_Inappropriate_Conversation,
	FL_Safety_Issue_No_Contact_Order,
	FL_Safety_Issue_DV,
	CD_Supervision_Level,
	Supervision_Level,
	Visit_Frequency,
	Visit_Duration_Hours,
	CD_Visitation_Referral_Type,
	Visitation_Referral_Type,
	cd_transportation_type,
	transportation_type */
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
