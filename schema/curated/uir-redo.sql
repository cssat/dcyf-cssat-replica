/* Erik's code, kept for now as reference */
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
), uir AS (
	SELECT uir."id" AS ID_SPROUT_Unusual_Incident_Report,
		uir."serviceReferralId" AS ID_SPROUT_Visit,
		uir."createdAt"::DATE AS DT_Reported,
		uir."date" AS DT_Incident,
		sr."dshsOffice" AS Office,
		sr."organizationId" AS ID_SPROUT_Visitation_Agency,
		org.name AS Visitation_Agency,
		org."contactName" AS Agency_Contact,
		org.phone AS Agency_Contact_Phone,
		org."famlinkId" AS ID_FamLink_Provider,
		CONCAT(sr."socialWorkerFirstName", ' ', sr."socialWorkerLastName") AS Name_DCYF_Worker,
		sr."caseNumber" AS ID_Case_FamLink,
		CONCAT(sr."caseDetailsFirstName", ' ', sr."caseDetailsLastName") AS Case_Name_FamLink,
		CONCAT(uir."staffNotifiedFirstName", ' ', uir."staffNotifiedLastName") AS Name_DCYF_Staff_Reported_To,
		uir."staffNotifiedPhone" AS Phone_DCYF_Staff_Reported_To,
		uir."staffNotifiedEmail" AS Email_DCYF_Staff_Reported_To,
		uir."staffNotifiedByMeans" AS Contact_Method,
		NULL AS CD_Contact_Method,
		CONCAT(sid."firstName", ' ', sid."lastName") AS Submitted_By_Name,
		CONCAT(aid."firstName", ' ', aid."lastName") AS Approved_By_Name,
		uir."approvedAt" AS DT_approved
	FROM staging."UnusualIncidentReports" uir
	JOIN staging."ServiceReferrals" AS sr
		ON uir."serviceReferralId" = sr.id
		AND sr."isCurrentVersion"
		AND sr."deletedAt" IS NULL
	JOIN staging."Organizations" AS org
		ON org.id = sr."organizationId"
	JOIN staging."Users" AS aid
		ON uir."approvedById" = aid.id
	JOIN staging."Users" AS sid
		ON uir."submittedById" = sid.id
	WHERE uir."serviceReferralId" NOT IN (SELECT id FROM sr_non_dcyf)
), uir_participants AS (
SELECT uip."id" AS ID_SPROUT_Unusual_Incident_Report_Participant,
		uip."unusualIncidentReportId" AS ID_SPROUT_Unusual_Incident_Report,
    	NULL AS ID_Person,
		CONCAT(uip."firstName", ' ', uip."lastName") AS Name,
    	uip."involvedPartyTypeId" AS CD_Role,
    	uip."otherInvolvedPartyType" AS Role,
		uip."birthDate" AS Birthdate,
		EXTRACT(YEAR FROM age(uir.DT_Incident, uip."birthDate")) AS Age
	FROM staging."UnusualIncidentInvolvedParties" AS uip
    JOIN uir AS uir
        ON uip."unusualIncidentReportId" = uir.ID_SPROUT_Unusual_Incident_Report
	WHERE uip."deletedAt" IS NULL
		AND uir.DT_Incident IS NOT NULL
), uir_actions AS (
	SELECT
    	uia."id" AS ID_SPROUT_Unusual_Incident_Report_Actions,
    	uia."unusualIncidentReportId" AS ID_SPROUT_Unusual_Incident_Report,
    	uia."actionTakenId" AS CD_Action_Taken,
    	uiat.name AS Action_Taken
	FROM staging."UnusualIncidentReportActions" AS uia
	JOIN staging."UnusualIncidentActionTypes" AS uiat
		ON uia."actionTakenId" = uiat.id
    JOIN uir AS uir
        ON uia."unusualIncidentReportId" = uir.ID_SPROUT_Unusual_Incident_Report
	WHERE uia."deletedAt" IS NULL
)
