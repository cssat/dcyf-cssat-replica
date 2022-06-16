DROP MATERIALIZED VIEW IF EXISTS dcyf.visit_report_dcyf;
CREATE MATERIALIZED VIEW IF NOT EXISTS dcyf.visit_report_dcyf AS

WITH visitation_referral AS (
SELECT "ID_Visitation_Referral",
	"Region",
	"CD_Region",
	"Office",
	"CD_Office",
	"ID_Worker",
	"Worker_Name",
	"DT_First_Visit_Scheduled",
	"Parent_Count",
	"Child_Count"
	FROM dcyf.visitation_referral
), child_referral_episode AS (
	SELECT "ID_Visitation_Referral",
		"Outcome_72_Hour_Visit",
		"CD_Outcome_72_Hour_Visit"
	FROM (
		SELECT "ID_Visitation_Referral",
		"Outcome_72_Hour_Visit",
		"CD_Outcome_72_Hour_Visit",
		MIN("CD_Outcome_72_Hour_Visit") OVER (PARTITION BY "ID_Visitation_Referral") min_outcome
	FROM (
		SELECT DISTINCT "ID_Visitation_Referral",
		"Outcome_72_Hour_Visit",
		CASE WHEN "Outcome_72_Hour_Visit" = 'Visit Completed Within 3 Days of Placement' THEN 1 
		WHEN "Outcome_72_Hour_Visit" = 'Cancelled or Missed Visit (Visit Report Needed)' THEN 2
		WHEN "Outcome_72_Hour_Visit" = 'Family Contact Information Incorrect' THEN 3
		WHEN "Outcome_72_Hour_Visit" = 'Caregiver Contact Information Incorrect' THEN 4
		WHEN "Outcome_72_Hour_Visit" = 'Unresponsive Parents' THEN 5
		WHEN "Outcome_72_Hour_Visit" = 'Unresponsive Caregivers' THEN 6
		WHEN "Outcome_72_Hour_Visit" = 'Conflict with Parent or Caregiver Schedule' THEN 7
		WHEN "Outcome_72_Hour_Visit" = 'Parent Refused' THEN 8
		WHEN "Outcome_72_Hour_Visit" = 'Child Refused' THEN 9
		WHEN "Outcome_72_Hour_Visit" = 'Placement Refused' THEN 10
		WHEN "Outcome_72_Hour_Visit" = 'Attorney Refused for Parent' THEN 11
		WHEN "Outcome_72_Hour_Visit" = 'Child Returned Home' THEN 12
		WHEN "Outcome_72_Hour_Visit" = 'Unable to Coordinate Visit with Incarcerated Parent' THEN 13
		WHEN "Outcome_72_Hour_Visit" = 'DCYF Canceled Referral' THEN 14
		WHEN "Outcome_72_Hour_Visit" = 'Not a 72 Hour Referral' THEN 15
		WHEN "Outcome_72_Hour_Visit" = 'Referral Received Post 72 Hours' THEN 16
		WHEN "Outcome_72_Hour_Visit" = 'Tribe Assumed Jurisdiction' THEN 17
		ELSE NULL END AS "CD_Outcome_72_Hour_Visit"
		FROM dcyf.child_referral_episode) cre ) crex
	WHERE min_outcome = "CD_Outcome_72_Hour_Visit"
), visit_observation AS (
SELECT id,
	CASE WHEN parent_on_time ->> 'yes' = 'true' THEN 1 
	WHEN parent_on_time ->> 'no' = 'true' THEN 0 
	WHEN parent_on_time ->> 'na' = 'true' THEN 2 
	ELSE NULL END AS "FL_Parent_On_Time",
	CASE WHEN child_on_time ->> 'yes' = 'true' THEN 1 
	WHEN child_on_time ->> 'no' = 'true' THEN 0 
	WHEN child_on_time ->> 'na' = 'true' THEN 2 
	ELSE NULL END AS "FL_Child_On_Time",
	CASE WHEN parent_entire_visit ->> 'yes' = 'true' THEN 1 
	WHEN parent_entire_visit ->> 'no' = 'true' THEN 0 
	WHEN parent_entire_visit ->> 'na' = 'true' THEN 2 
	ELSE NULL END AS "FL_Parent_Entire_Visit",
	CASE WHEN parent_ready_meet_needs ->> 'yes' = 'true' THEN 1 
	WHEN parent_ready_meet_needs ->> 'no' = 'true' THEN 0 
	WHEN parent_ready_meet_needs ->> 'na' = 'true' THEN 2 
	ELSE NULL END AS "FL_Parent_Ready_Meet_Needs",
	CASE WHEN parent_met_needs ->> 'yes' = 'true' THEN 1 
	WHEN parent_met_needs ->> 'no' = 'true' THEN 0 
	WHEN parent_met_needs ->> 'na' = 'true' THEN 2 
	ELSE NULL END AS "FL_Parent_Met_Needs",
	CASE WHEN parent_played ->> 'yes' = 'true' THEN 1 
	WHEN parent_played ->> 'no' = 'true' THEN 0 
	WHEN parent_played ->> 'na' = 'true' THEN 2 
	ELSE NULL END AS "FL_Parent_Played",
	CASE WHEN parent_set_limits ->> 'yes' = 'true' THEN 1 
	WHEN parent_set_limits ->> 'no' = 'true' THEN 0 
	WHEN parent_set_limits ->> 'na' = 'true' THEN 2 
	ELSE NULL END AS "FL_Parent_Set_Limits",
	CASE WHEN parent_helped_say_goodbye ->> 'yes' = 'true' THEN 1 
	WHEN parent_helped_say_goodbye ->> 'no' = 'true' THEN 0 
	WHEN parent_helped_say_goodbye ->> 'na' = 'true' THEN 2 
	ELSE NULL END AS "FL_Parent_Helped_Say_Goodbye",
	CASE WHEN location_no_safety_hazards ->> 'yes' = 'true' THEN 1 
	WHEN location_no_safety_hazards ->> 'no' = 'true' THEN 0 
	WHEN location_no_safety_hazards ->> 'na' = 'true' THEN 2 
	ELSE NULL END AS "FL_Location_No_Safety_Hazards",
	CASE WHEN supervision_intervention ->> 'yes' = 'true' THEN 1 
	WHEN supervision_intervention ->> 'no' = 'true' THEN 0 
	WHEN supervision_intervention ->> 'na' = 'true' THEN 2 
	ELSE NULL END AS "FL_Supervisor_Intervention",
	CASE WHEN unsual_incidents ->> 'yes' = 'true' THEN 1 
	WHEN unsual_incidents ->> 'no' = 'true' THEN 0 
	WHEN unsual_incidents ->> 'na' = 'true' THEN 2 
	ELSE NULL END AS "FL_Unusual_Incidents"
FROM dcyf.visit_reports,
	json_extract_path(observations, 'parentOnTimeForVisit', 'answers') parent_on_time,
	json_extract_path(observations, 'childOnTime', 'answers') child_on_time,
	json_extract_path(observations, 'parentStayedForEntireVisit', 'answers') parent_entire_visit,
	json_extract_path(observations, 'parentReadyToMeetNeedsOfTheChild', 'answers') parent_ready_meet_needs,
	json_extract_path(observations, 'parentMetChildsNeeds', 'answers') parent_met_needs,
	json_extract_path(observations, 'parentPlayedWithChild', 'answers') parent_played,
	json_extract_path(observations, 'parentSetLimitsWithChildAndManagedBehavior', 'answers') parent_set_limits,
	json_extract_path(observations, 'parentHelpedChildSayGoodbye', 'answers') parent_helped_say_goodbye,
	json_extract_path(observations, 'visitLocationFreeOfSafetyHazards', 'answers') location_no_safety_hazards,
	json_extract_path(observations, 'supervisorHadToInterveneToMaintainChildSafety', 'answers') supervision_intervention,
	json_extract_path(observations, 'unusualIncidents', 'answers') unsual_incidents
WHERE "isCurrentVersion"
	AND "deletedAt" IS NULL
), transport AS (
	SELECT id,
	SUM(td::real) distance,
	SUM(ts_diff)::int duration
	FROM (
		SELECT id,
		td,
		EXTRACT(EPOCH FROM (TO_TIMESTAMP(tet2, 'HH12:MI AM') - TO_TIMESTAMP(tst2, 'HH12:MI AM')))/60 ts_diff
		FROM (
			SELECT id,
			REPLACE(REPLACE(tet, 'pm', 'p'), 'p', 'PM') AS tet2,
			REPLACE(REPLACE(tst, 'pm', 'p'), 'p', 'PM') AS tst2,
			td
			FROM (
				SELECT id,
				REPLACE(REPLACE(LOWER(transport_end_time), 'am', 'a'), 'a', 'AM') AS tet,
				REPLACE(REPLACE(LOWER(transport_start_time), 'am', 'a'), 'a', 'AM') AS tst,
				REGEXP_REPLACE(transport_distance, '[^.,0-9]',  '', 'g') AS td
				FROM (
					SELECT
					id,
					CASE WHEN transport_details ->> 'transportEndTime' = '0' THEN NULL 
					ELSE REPLACE(transport_details ->> 'transportEndTime', '00:', '12:') END AS transport_end_time,
					CASE WHEN transport_details ->> 'transportStartTime' = '0' THEN NULL 
					ELSE REPLACE(transport_details ->> 'transportStartTime', '00:', '12:') END AS transport_start_time, 
					CASE WHEN transport_details ->> 'transportDistance' = '' THEN NULL
					ELSE transport_details ->> 'transportDistance' END AS transport_distance
					FROM dcyf.visit_reports,
					json_array_elements("transportDetails") AS transport_details
					WHERE "isCurrentVersion"
					AND "deletedAt" IS NULL) x 
			) xx
		)xxx 
	) xxxx
	GROUP BY id
), visit_attendees AS (
	SELECT
	id,
	"serviceReferralId",
	visit_attendees ->> 'attendeeFirstName' first_name,
	visit_attendees ->> 'attendeeLastName' last_name,
	visit_attendees ->> 'attendeeRelationship' relationship
	FROM dcyf.visit_reports,
	json_array_elements("visitAttendees") AS visit_attendees
	WHERE "isCurrentVersion"
	AND "deletedAt" IS NULL
), referral_child AS (
	SELECT 
	id,
	child_details ->> 'childFirstName' first_name,
	child_details ->> 'childLastName' last_name,
	child_details ->> 'childFamlinkPersonID' person_id,
	'Child' AS relationship
	FROM dcyf.service_referrals,
	json_array_elements("childDetails") AS child_details
	WHERE "isCurrentVersion"
	AND "deletedAt" IS NULL
), referral_parent AS (
	SELECT 
	id,
	parent_guardian_details ->> 'parentGuardianFirstName' first_name,
	parent_guardian_details ->> 'parentGuardianLastName' last_name,
	parent_guardian_details ->> 'parentGuardianId' person_id,
	'Parent' AS relationship
	FROM dcyf.service_referrals,
	json_array_elements("parentGuardianDetails") AS parent_guardian_details
	WHERE "isCurrentVersion"
	AND "deletedAt" IS NULL
), referral_participants AS (
	SELECT * FROM referral_child
	UNION 
	SELECT * FROM referral_parent
), attendance AS (
	SELECT DISTINCT id,
	MAX(fl_p_missed) OVER (PARTITION BY id, referral_id) AS fl_parent_missed,
	MAX(fl_missed) OVER (PARTITION BY id, referral_id) AS fl_any_missed
	FROM (
	SELECT vr.id,
	vr."serviceReferralId" referral_id,
	vr."reportType" report_type,
	rp.first_name referral_first_name,
	rp.last_name referral_last_name,
	rp.relationship referral_relationship,
	rp.person_id referral_person_id,
	va.first_name report_first_name,
	va.last_name report_last_name,
	va.relationship report_relationship,
	CASE WHEN vr."reportType" = 'Missed-no-show' 
	OR (rp.relationship = 'Parent' 
		AND rp.first_name IS NOT NULL AND rp.last_name IS NOT NULL 
		AND va.first_name IS NULL AND va.last_name IS NULL) 
	THEN 1 ELSE 0 END AS fl_p_missed,
	CASE WHEN vr."reportType" = 'Missed-no-show' 
	OR (rp.first_name IS NOT NULL AND rp.last_name IS NOT NULL 
		AND va.first_name IS NULL AND va.last_name IS NULL) 
	THEN 1 ELSE 0 END AS fl_missed
	FROM (SELECT id, "serviceReferralId", "reportType" FROM dcyf.visit_reports WHERE "isCurrentVersion" AND "deletedAt" IS NULL) vr
	LEFT JOIN referral_participants rp
	ON vr."serviceReferralId" = rp.id
	LEFT JOIN visit_attendees va
	ON vr.id = va.id
	AND rp.first_name = va.first_name
	AND rp.last_name = va.last_name
	AND rp.relationship = va.relationship) x
), supervisors AS (
	SELECT id,
	"ID_Visit_Supervisor",
	"Visit_Supervisor_Name"
	FROM (
		SELECT vr.id, 
		u.id "ID_Visit_Supervisor",
		CONCAT(u."firstName", ' ', u."lastName") "Visit_Supervisor_Name",
		ROW_NUMBER() OVER (PARTITION BY vr.id) AS row_num
		FROM dcyf.visit_reports vr
		LEFT OUTER JOIN replica."UserAssignments" ua
		ON vr."versionId" = ua."assignmentId"
		LEFT OUTER JOIN replica."Users" u
		ON ua."userId" = u.id
		WHERE vr."isCurrentVersion"
		AND vr."deletedAt" IS NULL) x
	WHERE row_num = 1
), referrals AS (
	SELECT id, 
	"formVersion", 
	"serviceType" 
	FROM dcyf.service_referrals 
	WHERE "isCurrentVersion"
	AND "deletedAt" IS NULL
	AND "formVersion" = 'Ingested'
) 
SELECT visit_reports.id "ID_Visit",
 	visit_reports."serviceReferralId" "ID_Visitation_Referral",
	visit_reports."caseNumber" "ID_Case",
	visitation_referral."CD_Region",
	visitation_referral."Region",
	visitation_referral."CD_Office",
	visitation_referral."Office",
	visitation_referral."ID_Worker",
	visitation_referral."Worker_Name",
	supervisors."ID_Visit_Supervisor",
	supervisors."Visit_Supervisor_Name",
	visitation_referral."DT_First_Visit_Scheduled",
	visit_reports.date "DT_Visit_Start",
	visit_reports."time" "Time_Visit_Start",
	visit_reports.date "DT_Visit_Stop",
    visit_reports."endTime" "Time_Visit_Stop",
	CASE WHEN visit_reports.virtual THEN 1
	WHEN NOT visit_reports.virtual OR visit_reports.virtual IS NULL THEN 2
	WHEN visit_reports."reportType" = 'Missed-no-show' THEN 3
	END AS "CD_Visit_Modality",
	CASE WHEN visit_reports.virtual THEN 'Virtual'
	WHEN NOT visit_reports.virtual OR visit_reports.virtual IS NULL THEN 'In-person'
	WHEN visit_reports."reportType" = 'Missed-no-show' THEN 'Missed-no-show'
	END AS "Visit_Modality",
	CASE WHEN visit_reports.virtual THEN 1 ELSE 0 END AS "FL_Virtual",
	CASE WHEN NOT visit_reports.virtual OR visit_reports.virtual IS NULL THEN 1 ELSE 0 END AS "FL_In_Person",
	attendance.fl_parent_missed "FL_Any_Parent_Missed",
	attendance.fl_any_missed "FL_Any_Missed",
	visit_observation."FL_Parent_On_Time",
	visit_observation."FL_Child_On_Time",
	visit_observation."FL_Parent_Entire_Visit",
	visit_observation."FL_Parent_Ready_Meet_Needs",
	visit_observation."FL_Parent_Met_Needs",
	visit_observation."FL_Parent_Played",
	visit_observation."FL_Parent_Set_Limits",
	visit_observation."FL_Parent_Helped_Say_Goodbye",
	visit_observation."FL_Location_No_Safety_Hazards",
	visit_observation."FL_Supervisor_Intervention",
	visit_observation."FL_Unusual_Incidents",
    transport.distance "Travel_Distance_Child_To_Visit",
    transport.duration "Travel_Duration_Child_To_Visit",
	visitation_referral."Parent_Count" "Parent_Count",
	visitation_referral."Child_Count" "Child_Count",
	CASE WHEN visit_reports.state = 'approved' THEN 1
	WHEN visit_reports.state = 'in-progress' THEN 2 
	WHEN visit_reports.state = 'in-review' THEN 3 END AS "CD_Status",
	visit_reports.state "Status",
	CASE WHEN visit_reports.state = 'approved' THEN 1 ELSE 0 END AS "FL_Approved",
	CASE WHEN visit_reports."visitLocationType" = 'Provider site' THEN 1
	WHEN visit_reports."visitLocationType" = 'Parent home' THEN 2
	WHEN visit_reports."visitLocationType" = 'Administrative office' THEN 3
	WHEN visit_reports."visitLocationType" = 'Relative home' THEN 4
	WHEN visit_reports."visitLocationType" = 'Park' THEN 5
	WHEN visit_reports."visitLocationType" = 'Restaurant' THEN 6
	WHEN visit_reports."visitLocationType" = 'Library' THEN 7
	WHEN visit_reports."visitLocationType" = 'Other' THEN 8
	END AS "CD_Location_Type",
	visit_reports."visitLocationType" "Location_Type",
	CASE WHEN visit_reports."visitType" = 'Unsupervised' THEN 1
	WHEN visit_reports."visitType" = 'Monitored' THEN 2
	WHEN visit_reports."visitType" = 'Supervised' THEN 3
	WHEN visit_reports."visitType" = 'Transport Only' THEN 4
	WHEN visit_reports."visitType" = 'Supported Visitation' THEN 5
	END AS "CD_Supervision Type",
	visit_reports."visitType" "Supervision_Type",
	CASE WHEN referrals."serviceType" = 'Parent / Child' THEN 1
	WHEN referrals."serviceType" = 'Parent / Child with Transportation' THEN 2
	WHEN referrals."serviceType" = 'Sibling' THEN 3
	WHEN referrals."serviceType" = 'Sibling with Transportation' THEN 4
	WHEN referrals."serviceType" = 'Transportation Only' THEN 5
	END AS "CD_Visit_Type",
	referrals."serviceType" "Visit_Type",
	child_referral_episode."CD_Outcome_72_Hour_Visit",
	child_referral_episode."Outcome_72_Hour_Visit",
	now() "DT_View_Refreshed"
   FROM dcyf.visit_reports 
   LEFT OUTER JOIN referrals
   ON visit_reports."serviceReferralId" = referrals.id
   LEFT OUTER JOIN visitation_referral
   ON visit_reports."serviceReferralId" = visitation_referral."ID_Visitation_Referral"
   LEFT OUTER JOIN child_referral_episode
   ON visit_reports."serviceReferralId" = child_referral_episode."ID_Visitation_Referral"
   LEFT OUTER JOIN visit_observation
   ON visit_reports.id = visit_observation.id
   LEFT OUTER JOIN transport
   ON visit_reports.id = transport.id
   LEFT OUTER JOIN attendance
   ON visit_reports.id = attendance.id
   LEFT OUTER JOIN supervisors
   ON visit_reports.id = supervisors.id
   WHERE visit_reports."deletedAt" IS NULL 
   AND visit_reports."isCurrentVersion"
   AND referrals."formVersion" = 'Ingested';