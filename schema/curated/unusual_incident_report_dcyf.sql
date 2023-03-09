-- DROP MATERIALIZED VIEW IF EXISTS dcyf.unusual_incident_report_dcyf CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS dcyf.unusual_incident_report_dcyf
TABLESPACE pg_default
AS
 WITH org AS (
         SELECT o.id,
            o.name
           FROM dcyf.organizations o
        )
 SELECT uir.id AS id_unusual_incident_report,
    uir."serviceReferralId" AS id_visitation_referral,
    uir."incidentNarrative" AS incident_narrative,
    uir."createdAt" AS dt_reported,
    uir.date AS dt_incident,
    sr."dshsOffice" AS office,
    sr."organizationId" AS id_provider_sprout,
    org.provider_name,
    org.provider_contact_name,
    org.provider_contact_phone,
    org.id_famlink_provider id_famlink_provider,
    concat(sr."socialWorkerFirstName", ' ', sr."socialWorkerLastName") AS name_dcyf_worker,
    sr."caseNumber" AS id_case_famlink,
    concat(sr."caseDetailsFirstName", ' ', sr."caseDetailsLastName") AS case_name_famlink,
    concat(uir."staffNotifiedFirstName", ' ', uir."staffNotifiedLastName") AS name_dcyf_staff_reported_to,
    uir."staffNotifiedPhone" AS phone_dcyf_staff_reported_to,
    uir."staffNotifiedEmail" AS email_dcyf_staff_reported_to,
    uir."staffNotifiedByMeans" AS contact_method,
    concat(sid."firstName", ' ', sid."lastName") AS submitted_by_name,
    concat(aid."firstName", ' ', aid."lastName") AS approved_by_name,
    uir."approvedAt"::date AS dt_approved,
    now() AS dt_view_refreshed
   FROM dcyf.unusual_incident_reports uir
     INNER JOIN dcyf.service_referrals sr
	 ON uir."serviceReferralId" = sr.id AND sr."isCurrentVersion" AND sr."deletedAt" IS NULL AND sr."formVersion" = 'Ingested'
     JOIN dcyf.sprout_providers org ON org.id_provider_sprout = sr."organizationId"
     JOIN replica."Users" aid ON uir."approvedById" = aid.id
     JOIN replica."Users" sid ON uir."submittedById" = sid.id
