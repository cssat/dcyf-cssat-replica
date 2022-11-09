DROP MATERIALIZED VIEW IF EXISTS dcyf.visit_report_participant;
CREATE MATERIALIZED VIEW IF NOT EXISTS dcyf.visit_report_participant AS

WITH referral_child AS (
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
	SELECT DISTINCT *,
	person_id::bigint id_person,
	last_name || ', ' || first_name participant_name
	FROM (
	SELECT * FROM referral_child
	UNION 
	SELECT * FROM referral_parent) u
), visit_attendees AS (
	SELECT DISTINCT
	id,
	"serviceReferralId",
	"reportType",
	"cancellationType",
	"causedBy",
	visit_attendees ->> 'attendeeFirstName' first_name,
	visit_attendees ->> 'attendeeLastName' last_name,
	visit_attendees ->> 'attendeeRelationship' relationship
	FROM dcyf.visit_reports,
	json_array_elements("visitAttendees") AS visit_attendees
	WHERE "isCurrentVersion"
	AND "deletedAt" IS NULL
), visit_participants AS (
	SELECT 
	va.id "ID_Visit_Report",
	vrp."ID_Visitation_Referral_Participant",
	va."serviceReferralId" "ID_Visitation_Referral",
	CASE WHEN va."cancellationType" = 'No-show' AND va."causedBy" = va.relationship THEN 1
	ELSE 0 END AS "FL_No_Show",
	rp.person_id "ID_Person",
	rp.participant_name,
	CASE WHEN fv."serviceType" = 'Sibling' AND va.relationship = 'Child' THEN 'Sibling'
	ELSE va.relationship END AS "Role"
	FROM visit_attendees va
	LEFT JOIN (SELECT id, "serviceType", "formVersion" FROM dcyf.service_referrals WHERE "isCurrentVersion" AND "deletedAt" IS NULL) fv
	ON va."serviceReferralId" = fv.id
	LEFT JOIN referral_participants rp
	ON va."serviceReferralId" = rp.id
	AND va.relationship = rp.relationship
	AND va.first_name = rp.first_name
	AND va.last_name = rp.last_name
	LEFT JOIN (SELECT "ID_Visitation_Referral_Participant", "ID_Visitation_Referral", "ID_Person", "Role" from dcyf.visitation_referral_participant) vrp
	ON va."serviceReferralId" = vrp."ID_Visitation_Referral"
	AND rp.id_person = vrp."ID_Person"
	AND va.relationship = vrp."Role"
	WHERE "formVersion" = 'Ingested'
), visit_report_participant AS (
	SELECT *,
	CASE WHEN "Role" = 'CASA' THEN 3
	WHEN "Role" = 'Case Manager' THEN 4
	WHEN "Role" = 'Child' THEN 1
	WHEN "Role" = 'CPA Worker' THEN 5
	WHEN "Role" = 'Custodial Parent' THEN 6
	WHEN "Role" = 'Family Member' THEN 7
	WHEN "Role" = 'Foster Parent' THEN 8
	WHEN "Role" = 'Guardian Ad Litem' THEN 9
	WHEN "Role" = 'Parent' THEN 2
	WHEN "Role" = 'Provider' THEN 10
	WHEN "Role" = 'Relative Caregiver' THEN 11
	WHEN "Role" = 'Sibling' THEN 12
	WHEN "Role" = 'Social Worker' THEN 13
	WHEN "Role" = 'Other' THEN 14
	WHEN "Role" IS NULL THEN 15
	ELSE 0 END AS "CD_Role"
	FROM visit_participants
	ORDER BY "ID_Visit_Report", "ID_Person")
	
SELECT
CASE WHEN "ID_Person" IS NULL THEN concat_ws('_'::varchar, concat('R', ROW_NUMBER() OVER()), "ID_Visit_Report", "CD_Role") 
ELSE concat_ws('_'::varchar, "ID_Person", "ID_Visit_Report", "CD_Role") 
END AS "ID_Visit_Report_Participant",
"ID_Visit_Report",
"ID_Visitation_Referral_Participant"::varchar,
"ID_Visitation_Referral",
"FL_No_Show"::smallint,
"ID_Person"::int,
participant_name "Participant_Name",
"CD_Role",
"Role"::varchar,
now() "DT_View_Refreshed"
FROM visit_report_participant