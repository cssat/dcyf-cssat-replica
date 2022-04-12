-- View: dcyf.child_removal_supervision_level

DROP MATERIALIZED VIEW IF EXISTS dcyf.child_removal_supervision_level;

CREATE MATERIALIZED VIEW IF NOT EXISTS dcyf.child_removal_supervision_level
TABLESPACE pg_default AS 
WITH child_referral_supervision AS (
	SELECT "ID_Child_Removal_Episode",
	"ID_Referral",
	"DT_Start",
	"DT_Referral_Resolved",
	"levelOfSupervision" supervision_level_referral
	FROM dcyf.child_referral_episode
	LEFT OUTER JOIN (
	SELECT id,
	"levelOfSupervision"
	FROM replica."ServiceReferrals"
	WHERE "deletedAt" IS NULL
	AND "isCurrentVersion") sl
	ON "ID_Referral" = id
	ORDER BY "ID_Child_Removal_Episode", "ID_Referral"
), child_report_supervision AS (
	SELECT *
	FROM (
		SELECT "serviceReferralId" id_referral,
		id id_report,
		date dt_report,
		"visitType" supervision_level_report,
		MIN(id) OVER (PARTITION BY "serviceReferralId", "visitType" ORDER BY id) AS id_supervision_level_report
		FROM replica."VisitReports"
		WHERE "deletedAt" IS NULL
		AND "isCurrentVersion"
		AND "visitType" IS NOT NULL) sr
	WHERE id_report = id_supervision_level_report
	ORDER BY id_referral, id_report
), child_report_supervision_change AS (
	SELECT DISTINCT
	id_referral,
	id_report,
	dt_report,
	supervision_level_report
	FROM child_referral_supervision
	LEFT OUTER JOIN child_report_supervision
	ON "ID_Referral" = id_referral
	WHERE supervision_level_report IS NOT NULL
	AND supervision_level_referral != supervision_level_report
	AND NOT (supervision_level_referral IN ('Unsupervised', 'Transport Only') AND supervision_level_report IN ('Unsupervised', 'Transport Only'))
	ORDER BY id_referral, id_report
), child_referral_report_supervision AS (
	SELECT *,
	CAST("ID_Referral" AS varchar) id_vc_referral,
	MIN("DT_Start") OVER (PARTITION BY "ID_Child_Removal_Episode", supervision_level_referral) AS "DT_Supervision_Level_Start_Referral",
	MAX("DT_Referral_Resolved") OVER (PARTITION BY "ID_Child_Removal_Episode", supervision_level_referral) AS "DT_Supervision_Level_End_Referral",
	CASE WHEN "ID_Referral" = MIN("ID_Referral") OVER (PARTITION BY "ID_Child_Removal_Episode", supervision_level_referral)
	THEN 1 ELSE 0 END AS fl_first_referral_supervision_level
	FROM child_referral_supervision
	LEFT OUTER JOIN child_report_supervision_change
	ON "ID_Referral" = id_referral
), child_removal_supervision_level AS (
	SELECT 
	dcyf.make_int_pk("ID_Child_Removal_Episode" || id_vc_referral) AS "ID_Child_Removal_Supervision_Level",
	"ID_Child_Removal_Episode",
	"DT_Supervision_Level_Start_Referral",
	"DT_Supervision_Level_End_Referral",
	"ID_Referral" "ID_Referral_Source",
	supervision_level_referral "Supervision_Level_Referral",
	dt_report "DT_Supervision_Level_Report",
	id_report "ID_Visit_Source",
	supervision_level_report "Supervision_Level_Report"
	FROM child_referral_report_supervision
	WHERE fl_first_referral_supervision_level = 1
	OR supervision_level_report IS NOT Null
	ORDER BY "ID_Child_Removal_Episode", "ID_Referral"
)

SELECT * FROM child_removal_supervision_level

WITH DATA;

ALTER TABLE IF EXISTS dcyf.child_removal_supervision_level
    OWNER TO aptible;

GRANT ALL ON TABLE dcyf.child_removal_supervision_level TO aptible;
GRANT SELECT ON TABLE dcyf.child_removal_supervision_level TO dcyf_users;