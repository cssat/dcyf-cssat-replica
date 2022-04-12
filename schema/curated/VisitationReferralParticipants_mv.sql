-- View: dcyf.visitation_referral_participants

DROP MATERIALIZED VIEW IF EXISTS dcyf.visitation_referral_participants;

CREATE MATERIALIZED VIEW IF NOT EXISTS dcyf.visitation_referral_participants
TABLESPACE pg_default AS 
WITH dcyf_orgs AS (
	SELECT o.id 
	FROM replica."Organizations" o
    JOIN replica."OrganizationContracts" oc 
	ON o.id = oc."contractedOrganizationId"
	AND oc."contractOwnerId" = 21
), child_details AS (
 SELECT id,
	child_details->>'childFamlinkPersonID' id_person,
	child_details->>'childFirstName' first_name,
	child_details->>'childLastName' last_name,
	CASE WHEN child_details->>'childFamlinkPersonID' IS NOT NULL THEN 'Child' END AS role 
	FROM replica."ServiceReferrals",
	json_array_elements("childDetails") child_details
	WHERE "isCurrentVersion"
 	AND "deletedAt" IS NULL
 	AND "formVersion" = 'Ingested'
 	AND "organizationId" IN (SELECT id FROM dcyf_orgs)
), parent_details AS (
	SELECT id,
	parent_details->>'parentGuardianId' id_person,
 	parent_details->>'parentGuardianFirstName' first_name,
 	parent_details->>'parentGuardianLastName' last_name,
	CASE WHEN parent_details->>'parentGuardianId' IS NOT NULL THEN 'Parent' END AS role 
	FROM replica."ServiceReferrals",
	json_array_elements("parentGuardianDetails") parent_details
	WHERE "isCurrentVersion"
 	AND "deletedAt" IS NULL
 	AND "formVersion" = 'Ingested'
 	AND "organizationId" IN (SELECT id FROM dcyf_orgs)
), person_details AS (
	SELECT *
	FROM child_details
	UNION
	SELECT *
	FROM parent_details
), safety_issues AS (
 	SELECT id,
 	safety_issues->>'issueExhibitorId' id_person,
 	safety_issues->>'angerOutbursts' "FL_Safety_Issue_Anger_Outburst",
 	safety_issues->>'inappropriateTouching' "FL_Safety_Issue_Inappropriate_Touch",
 	safety_issues->>'inappropriateConversation' "FL_Safety_Issue_Inappropriate_Conversation",
 	safety_issues->>'substanceAbuse' "FL_Safety_Issue_Substance_Abuse",
 	safety_issues->>'leaveWithChild' "FL_Safety_Issue_Try_To_Leave",
 	safety_issues->>'threateningBehavior' "FL_Safety_Issue_Threatening_Behavior",
 	safety_issues->>'medicallyComplex' "FL_Safety_Issue_Medically_Complex",
 	safety_issues->>'restrainingOrder' "FL_Safety_Issue_No_Contact_Order",
 	safety_issues->>'domesticViolence' "FL_Safety_Issue_DV",
	safety_issues->>'other' "FL_Safety_Issue_Other",
	"safetyIssuesExplanation" "Safety_Issue_Explanation"
 	FROM replica."ServiceReferrals",
 	json_array_elements("safetyIssues") safety_issues
 	WHERE "isCurrentVersion"
 	AND "deletedAt" IS NULL
 	AND "formVersion" = 'Ingested'
	AND safety_issues->>'issueExhibitorId' IS NOT NULL
), visitation_referral_participants AS (
	SELECT 
	dcyf.make_int_pk(person_details.id || person_details.id_person) AS "ID_Visitation_Referral_Participant",
	person_details.id "ID_Visitation_Referral",
	person_details.id_person "ID_Person",
	CONCAT(first_name, ' ', last_name) "Participant_Name",
	CASE WHEN role = 'Child' THEN 1 WHEN role = 'Parent' THEN 2 END AS "CD_Role",
	role "Role",
	CASE WHEN "FL_Safety_Issue_Anger_Outburst" = 'true' THEN 1 ELSE 0 END AS "FL_Safety_Issue_Anger_Outburst",
 	CASE WHEN "FL_Safety_Issue_Inappropriate_Touch" = 'true' THEN 1 ELSE 0 END AS "FL_Safety_Issue_Inappropriate_Touch",
 	CASE WHEN "FL_Safety_Issue_Inappropriate_Conversation" = 'true' THEN 1 ELSE 0 END AS "FL_Safety_Issue_Inappropriate_Conversation",
 	CASE WHEN "FL_Safety_Issue_Substance_Abuse" = 'true' THEN 1 ELSE 0 END AS "FL_Safety_Issue_Substance_Abuse",
 	CASE WHEN "FL_Safety_Issue_Try_To_Leave" = 'true' THEN 1 ELSE 0 END AS "FL_Safety_Issue_Try_To_Leave",
 	CASE WHEN "FL_Safety_Issue_Threatening_Behavior" = 'true' THEN 1 ELSE 0 END AS "FL_Safety_Issue_Threatening_Behavior",
 	CASE WHEN "FL_Safety_Issue_Medically_Complex" = 'true' THEN 1 ELSE 0 END AS "FL_Safety_Issue_Medically_Complex" ,
 	CASE WHEN "FL_Safety_Issue_No_Contact_Order" = 'true' THEN 1 ELSE 0 END AS "FL_Safety_Issue_No_Contact_Order",
 	CASE WHEN "FL_Safety_Issue_DV" = 'true' THEN 1 ELSE 0 END AS "FL_Safety_Issue_DV",
	CASE WHEN "FL_Safety_Issue_Other" = 'true' THEN 1 ELSE 0 END AS "FL_Safety_Issue_Other",
	"Safety_Issue_Explanation"
	FROM person_details
	LEFT OUTER JOIN safety_issues
	ON person_details.id = safety_issues.id
	AND person_details.id_person = safety_issues.id_person
	ORDER BY person_details.id, person_details.id_person)

SELECT * 		
FROM visitation_referral_participants

WITH DATA;

ALTER TABLE IF EXISTS dcyf.visitation_referral_participants
    OWNER TO aptible;

GRANT ALL ON TABLE dcyf.visitation_referral_participants TO aptible;
GRANT SELECT ON TABLE dcyf.visitation_referral_participants TO dcyf_users;