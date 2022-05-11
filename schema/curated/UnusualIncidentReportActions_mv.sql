-- View: dcyf.unusual_incident_report_actions_dcyf

-- DROP MATERIALIZED VIEW IF EXISTS dcyf.unusual_incident_report_actions_dcyf;

CREATE MATERIALIZED VIEW IF NOT EXISTS dcyf.unusual_incident_report_actions_dcyf
TABLESPACE pg_default
AS
 SELECT uira.id AS id_unusual_incident_report_actions,
    uira."unusualIncidentReportId" AS id_unusual_incident_report,
    uira."actionTakenId" AS cd_action_taken,
    uiat.name AS action_taken
   FROM dcyf.unusual_incident_report_actions uira
     LEFT JOIN dcyf.unusual_incident_action_types uiat ON uira."actionTakenId" = uiat.id
  WHERE uira."deletedAt" IS NULL
WITH NO DATA;

ALTER TABLE IF EXISTS dcyf.unusual_incident_report_actions_dcyf
    OWNER TO aptible;

GRANT ALL ON TABLE dcyf.unusual_incident_report_actions_dcyf TO aptible;
GRANT SELECT ON TABLE dcyf.unusual_incident_report_actions_dcyf TO dcyf_users;
