DROP MATERIALIZED VIEW IF EXISTS dcyf.child_removal_episode;
CREATE MATERIALIZED VIEW IF NOT EXISTS dcyf.child_removal_episode AS

WITH child_removal_episode AS (
	SELECT "ID_Child_Removal_Episode",
	"ID_First_Referral",
	"ID_First_Visit_Attended" AS "ID_First_Visit",
	"ID_Person",
	"DT_OPD",
	"DT_OPD_Imputed",
	"FL_Imputed",
	"DT_Start",
	"DT_OPD_Coalesced",
	"DT_Last_Referral_Resolved",
	"DT_First_Visit_Attended" AS "DT_First_Visit",
	CASE WHEN "DT_First_Reported" < "DT_First_Visit_Attended" THEN "DT_First_Reported" ELSE NULL END AS "DT_First_Visit_Attempt",
	"CD_First_Visit_Attended_Modality" AS "CD_First_Visit_Modality",
	CASE WHEN "CD_First_Visit_Attended_Modality" = 1 THEN 'Virtual'::varchar ELSE 'In Person'::varchar END AS "First_Visit_Modality",
	"Total_Referrals"::smallint "Total_Referrals",
	"FL_Last_Visitation_Ended" AS "FL_Visitation_Ended",
	"FL_First_Referral_72_Hour",
	CASE WHEN "First_Outcome_72_Hour_Visit" = 'Visit Completed Within 3 Days of Placement' THEN 1 
	WHEN "First_Outcome_72_Hour_Visit" = 'Cancelled or Missed Visit (Visit Report Needed)' THEN 2
	WHEN "First_Outcome_72_Hour_Visit" = 'Family Contact Information Incorrect' THEN 3
	WHEN "First_Outcome_72_Hour_Visit" = 'Caregiver Contact Information Incorrect' THEN 4
	WHEN "First_Outcome_72_Hour_Visit" = 'Unresponsive Parents' THEN 5
	WHEN "First_Outcome_72_Hour_Visit" = 'Unresponsive Caregivers' THEN 6
	WHEN "First_Outcome_72_Hour_Visit" = 'Conflict with Parent or Caregiver Schedule' THEN 7
	WHEN "First_Outcome_72_Hour_Visit" = 'Parent Refused' THEN 8
	WHEN "First_Outcome_72_Hour_Visit" = 'Child Refused' THEN 9
	WHEN "First_Outcome_72_Hour_Visit" = 'Placement Refused' THEN 10
	WHEN "First_Outcome_72_Hour_Visit" = 'Attorney Refused for Parent' THEN 11
	WHEN "First_Outcome_72_Hour_Visit" = 'Child Returned Home' THEN 12
	WHEN "First_Outcome_72_Hour_Visit" = 'Unable to Coordinate Visit with Incarcerated Parent' THEN 13
	WHEN "First_Outcome_72_Hour_Visit" = 'DCYF Canceled Referral' THEN 14
	WHEN "First_Outcome_72_Hour_Visit" = 'Not a 72 Hour Referral' THEN 15
	WHEN "First_Outcome_72_Hour_Visit" = 'Referral Received Post 72 Hours' THEN 16
	WHEN "First_Outcome_72_Hour_Visit" = 'Tribe Assumed Jurisdiction' THEN 17
	ELSE NULL END AS "CD_Outcome_72_Hour_Visit",
	"First_Outcome_72_Hour_Visit" AS "Outcome_72_Hour_Visit"
	FROM (
	SELECT *,
	MIN(id_first_referral) OVER (PARTITION BY "ID_Child_Removal_Episode") AS "ID_First_Referral",
	MIN("ID_First_Visit") OVER (PARTITION BY "ID_Child_Removal_Episode") AS "ID_First_Visit_Attended",
	FIRST_VALUE("DT_First_Visit") OVER (PARTITION BY "ID_Child_Removal_Episode" ORDER BY "ID_First_Visit") AS "DT_First_Visit_Attended",
	FIRST_VALUE("DT_First_Report") OVER (PARTITION BY "ID_Child_Removal_Episode" ORDER BY "ID_First_Visit") AS "DT_First_Reported",
	MAX(dt_last_referral_resolved) OVER (PARTITION BY "ID_Child_Removal_Episode") AS "DT_Last_Referral_Resolved",
	MIN(dt_first_start) OVER (PARTITION BY "ID_Child_Removal_Episode") AS "DT_First_Start",
	MIN(cd_first_visit_modality) OVER (PARTITION BY "ID_Child_Removal_Episode") AS "CD_First_Visit_Attended_Modality",
	MAX(fl_visitation_ended) OVER (PARTITION BY "ID_Child_Removal_Episode") AS "FL_Last_Visitation_Ended",
	MAX(fl_referral_72_hr) OVER (PARTITION BY "ID_Child_Removal_Episode") AS "FL_First_Referral_72_Hour",
	FIRST_VALUE(outcome_72_hour_visit) OVER (PARTITION BY "ID_Child_Removal_Episode" ORDER BY "FL_Referral_72_Hour" DESC, "ID_Visitation_Referral") 
	AS "First_Outcome_72_Hour_Visit"
	FROM (
		SELECT *,
		MIN("ID_Visitation_Referral") OVER (PARTITION BY "ID_Child_Removal_Episode") AS id_min_referral,
		CASE WHEN "ID_First_Visit" = MIN("ID_First_Visit") OVER (PARTITION BY "ID_Child_Removal_Episode") 
		THEN "ID_Visitation_Referral" ELSE NULL END AS id_first_referral,
		CASE WHEN "ID_First_Visit" = MIN("ID_First_Visit") OVER (PARTITION BY "ID_Child_Removal_Episode") 
		THEN "DT_Start" ELSE NULL END AS dt_first_start,
		CASE WHEN "ID_First_Visit" = MIN("ID_First_Visit") OVER (PARTITION BY "ID_Child_Removal_Episode") 
		THEN "CD_First_Visit_Modality" ELSE NULL END AS cd_first_visit_modality,
		CASE WHEN "ID_Visitation_Referral" = MAX("ID_Visitation_Referral") OVER (PARTITION BY "ID_Child_Removal_Episode") 
		THEN "DT_Referral_Resolved" ELSE NULL END AS dt_last_referral_resolved,
		CASE WHEN "ID_Visitation_Referral" = MAX("ID_Visitation_Referral") OVER (PARTITION BY "ID_Child_Removal_Episode") 
		THEN "FL_Visitation_Ended" ELSE NULL END AS fl_visitation_ended,
		CASE WHEN "ID_Visitation_Referral" = MIN("ID_Visitation_Referral") OVER (PARTITION BY "ID_Child_Removal_Episode") 
		THEN "FL_Referral_72_Hour" ELSE NULL END AS fl_referral_72_hr,
		CASE WHEN "FL_Referral_72_Hour" = 1 THEN "Outcome_72_Hour_Visit" ELSE NULL END AS outcome_72_hour_visit
		FROM dcyf.child_referral_episode
		ORDER BY "ID_Child_Removal_Episode", "ID_Visitation_Referral", "ID_First_Visit") cre
	) crex
	WHERE "ID_Visitation_Referral" = id_min_referral
	ORDER BY "ID_Person", "DT_OPD_Coalesced"
)
SELECT *,
now() "DT_View_Refreshed"
FROM child_removal_episode;