-- View: dcyf.visitation_referral_participant

-- DROP MATERIALIZED VIEW IF EXISTS dcyf.visitation_referral_participant;

CREATE MATERIALIZED VIEW IF NOT EXISTS dcyf.visitation_referral_participant
TABLESPACE pg_default AS 
WITH dcyf_orgs AS (
	SELECT o.id 
	FROM replica."Organizations" o
    JOIN replica."OrganizationContracts" oc 
	ON o.id = oc."contractedOrganizationId"
	AND oc."contractOwnerId" = 21
), child_opd AS (
	SELECT DISTINCT id,
	child_details->>'childFamlinkPersonID' id_person,
	child_details->>'childOpd' dt_opd
	FROM replica."ServiceReferrals",
	json_array_elements("childDetails") child_details
	WHERE "isCurrentVersion"
 	AND "deletedAt" IS NULL
 	AND "formVersion" = 'Ingested'
 	AND "organizationId" IN (SELECT id FROM dcyf_orgs)
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
	SELECT DISTINCT *
	FROM child_details
	WHERE id_person IS NOT NULL
	UNION
	SELECT DISTINCT *
	FROM parent_details
	WHERE id_person IS NOT NULL
), dedup_person_details AS (
	SELECT DISTINCT *,
	CASE WHEN role = 'Child' THEN 1 WHEN role = 'Parent' THEN 2 END AS "CD_Role"
		FROM (
			SELECT
			id,
			id_person,
			CASE WHEN n_id > 1 AND role = 'Parent' AND dt_opd IS NOT NULL THEN 'Child' ELSE role END AS role,
			dt_opd,
			first_name,
			last_name
		FROM (
			SELECT
			COUNT(*) OVER (PARTITION BY person_details.id, person_details.id_person) AS n_id,
			person_details.id,
			person_details.id_person,
			role,
			dt_opd,
			first_name,
			last_name
			FROM person_details
			LEFT OUTER JOIN child_opd
			ON person_details.id = child_opd.id
			AND person_details.id_person = child_opd.id_person) pd
		) pdd
), safety_issues AS (
	SELECT * FROM (
	SELECT *,
	ROW_NUMBER(*) OVER (PARTITION BY id, id_person, issue_exhibitor_role) AS row_num
	FROM (
 	SELECT id,
 	safety_issues->>'issueExhibitorId' id_person,
	safety_issues->>'issueExhibitorRole' issue_exhibitor_role,
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
	"safetyIssuesExplanation"::varchar "Safety_Issue_Explanation"
 	FROM replica."ServiceReferrals",
 	json_array_elements("safetyIssues") safety_issues
 	WHERE "isCurrentVersion"
 	AND "deletedAt" IS NULL
 	AND "formVersion" = 'Ingested'
	AND safety_issues->>'issueExhibitorId' IS NOT NULL) si) si_distinct
	WHERE row_num = 1
), visitation_referral_participant AS (
	SELECT
	CONCAT_WS('_', dedup_person_details.id_person, dedup_person_details.id, "CD_Role") AS "ID_Visitation_Referral_Participant",
	dedup_person_details.id "ID_Visitation_Referral",
	dedup_person_details.id_person::int "ID_Person",
	CONCAT(first_name, ' ', last_name)::varchar "Participant_Name",
	"CD_Role",
	role::varchar "Role",
	dt_opd::date "DT_OPD",
	CASE WHEN "FL_Safety_Issue_Anger_Outburst" = 'true' THEN 1::smallint ELSE 0::smallint END AS "FL_Safety_Issue_Anger_Outburst",
 	CASE WHEN "FL_Safety_Issue_Inappropriate_Touch" = 'true' THEN 1::smallint ELSE 0::smallint END AS "FL_Safety_Issue_Inappropriate_Touch",
 	CASE WHEN "FL_Safety_Issue_Inappropriate_Conversation" = 'true' THEN 1::smallint ELSE 0::smallint END AS "FL_Safety_Issue_Inappropriate_Conversation",
 	CASE WHEN "FL_Safety_Issue_Substance_Abuse" = 'true' THEN 1::smallint ELSE 0::smallint END AS "FL_Safety_Issue_Substance_Abuse",
 	CASE WHEN "FL_Safety_Issue_Try_To_Leave" = 'true' THEN 1::smallint ELSE 0::smallint END AS "FL_Safety_Issue_Try_To_Leave",
 	CASE WHEN "FL_Safety_Issue_Threatening_Behavior" = 'true' THEN 1::smallint ELSE 0::smallint END AS "FL_Safety_Issue_Threatening_Behavior",
 	CASE WHEN "FL_Safety_Issue_Medically_Complex" = 'true' THEN 1::smallint ELSE 0::smallint END AS "FL_Safety_Issue_Medically_Complex" ,
 	CASE WHEN "FL_Safety_Issue_No_Contact_Order" = 'true' THEN 1::smallint ELSE 0::smallint END AS "FL_Safety_Issue_No_Contact_Order",
 	CASE WHEN "FL_Safety_Issue_DV" = 'true' THEN 1::smallint ELSE 0::smallint END AS "FL_Safety_Issue_DV",
	CASE WHEN "FL_Safety_Issue_Other" = 'true' THEN 1::smallint ELSE 0::smallint END AS "FL_Safety_Issue_Other",
	"Safety_Issue_Explanation"	
	FROM dedup_person_details
	LEFT OUTER JOIN safety_issues
	ON dedup_person_details.id = safety_issues.id
	AND dedup_person_details.id_person = safety_issues.id_person
	AND role = "issue_exhibitor_role"
	ORDER BY dedup_person_details.id, dedup_person_details.id_person)

SELECT * FROM visitation_referral_participant

WITH DATA;

ALTER TABLE IF EXISTS dcyf.visitation_referral_participant
    OWNER TO aptible;

GRANT ALL ON TABLE dcyf.visitation_referral_participant TO aptible;
GRANT SELECT ON TABLE dcyf.visitation_referral_participant TO dcyf_users;