-- View: dcyf.unusual_incident_report_dcyf

-- DROP MATERIALIZED VIEW IF EXISTS dcyf.unusual_incident_report_dcyf;

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
    uir."createdAt" AS dt_reported,
    uir.date AS dt_incident,
    sr."dshsOffice" AS office,
    sr."organizationId" AS id_provider_sprout,
    org.provider_name,
    org.provider_contact_name,
    org.provider_contact_phone,
    org.famlink_provider_id,
    concat(sr."socialWorkerFirstName", ' ', sr."socialWorkerLastName") AS name_dcyf_worker,
    sr."caseNumber" AS id_case_famlink,
    concat(sr."caseDetailsFirstName", ' ', sr."caseDetailsLastName") AS case_name_famlink,
    concat(uir."staffNotifiedFirstName", ' ', uir."staffNotifiedLastName") AS name_dcyf_staff_reported_to,
    uir."staffNotifiedPhone" AS phone_dcyf_staff_reported_to,
    uir."staffNotifiedEmail" AS email_dcyf_staff_reported_to,
    uir."staffNotifiedByMeans" AS contact_method,
    concat(sid."firstName", ' ', sid."lastName") AS submitted_by_name,
    concat(aid."firstName", ' ', aid."lastName") AS approved_by_name,
    uir."approvedAt"::date AS dt_approved
   FROM dcyf.unusual_incident_reports uir
     JOIN dcyf.service_referrals sr ON uir."serviceReferralId" = sr.id AND sr."isCurrentVersion" AND sr."deletedAt" IS NULL
     JOIN dcyf.sprout_providers org ON org.id_provider_sprout = sr."organizationId"
     JOIN replica."Users" aid ON uir."approvedById" = aid.id
     JOIN replica."Users" sid ON uir."submittedById" = sid.id
WITH DATA;

ALTER TABLE IF EXISTS dcyf.unusual_incident_report_dcyf
    OWNER TO aptible;

GRANT ALL ON TABLE dcyf.unusual_incident_report_dcyf TO aptible;
GRANT SELECT ON TABLE dcyf.unusual_incident_report_dcyf TO dcyf_users;
