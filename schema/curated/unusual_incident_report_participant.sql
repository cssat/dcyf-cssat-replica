with kids as(
SELECT DISTINCT
	id as id_visitation_referral
	, child_details ->> 'childFamlinkPersonID' person_id
	, lower(child_details ->> 'childFirstName') child_first
	, lower(child_details ->> 'childLastName') child_last
	FROM dcyf.service_referrals,
		json_array_elements("childDetails") AS child_details
	WHERE
	"deletedAt" is NULL
	and "isCurrentVersion"
	and child_details ->> 'childFamlinkPersonID' is not NULL
	and "formVersion"::text = 'Ingested'::text
)


select 
uiip.id as id_unusual_incident_report_participant
, uiip."unusualIncidentReportId" as id_unusual_incident_report
, kids.person_id::int as id_person
, uiip."lastName" || ', ' || uiip."firstName" as name
, uiip."involvedPartyTypeId" as cd_role
, pt."name" as "role"
, uiip."birthDate" as birthdate
, date_part('year', age(uir.dt_incident, uiip."birthDate"))::int as age
, now() as dt_view_refreshed

FROM dcyf.unusual_incident_involved_parties uiip

JOIN dcyf.unusual_incident_report_dcyf uir
ON uiip."unusualIncidentReportId" = uir.id_unusual_incident_report
AND uir.DT_Incident IS NOT NULL

LEFT JOIN kids
ON uir.id_visitation_referral = kids.id_visitation_referral
AND lower(uiip."lastName") = kids.child_last
AND lower(uiip."firstName") = kids.child_first
AND uiip."involvedPartyTypeId" = 1 --look here for ids for children only

LEFT JOIN dcyf.unusual_incident_involved_party_types pt
on uiip."involvedPartyTypeId" = pt."id" and pt."deletedAt" is NULL



/*

	
SELECT parent_guardian_details ->> 'parentGuardianId' person_id,
	FROM dcyf.service_referrals,
	json_array_elements("parentGuardianDetails") AS parent_guardian_details
*/
