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
	SELECT * FROM referral_child
	UNION 
	SELECT * FROM referral_parent
), visit_attendees AS (
	SELECT
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
	concat_ws('_'::text, rp.person_id, va.id) AS "ID_Visit_Report_Participant",
	va.id "ID_Visit_Report",
	vrp."ID_Visitation_Referral_Participant",
	va."serviceReferralId" "ID_Visitation_Referral",
	va."cancellationType",
	va."causedBy",
	va.relationship,
	CASE WHEN va."cancellationType" = 'No-show' AND va."causedBy" = va.relationship THEN 1
	ELSE 0 END AS "FL_No_Show",
	rp.person_id "ID_Person",
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
	LEFT JOIN (SELECT "ID_Visitation_Referral_Participant", "ID_Visitation_Referral", "ID_Person" from dcyf.visitation_referral_participant) vrp
	ON va."serviceReferralId" = vrp."ID_Visitation_Referral"
	AND rp.person_id = vrp."ID_Person"
	WHERE "formVersion" = 'Ingested')

--SELECT COUNT(*), relationship FROM visit_participants GROUP BY relationship
--SELECT * FROM visit_attendees WHERE "reportType" = 'Sibling' AND relationship = 'Child'
SELECT * FROM visit_participants