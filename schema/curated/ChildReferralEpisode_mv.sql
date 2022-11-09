DROP MATERIALIZED VIEW IF EXISTS dcyf.child_referral_episode CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS dcyf.child_referral_episode AS

WITH referrals AS (
  SELECT
	id,
    "startDate",
    "endDate",
    "createdAt",
    "referralState",
	"routingOrganizationId"
  FROM
    replica."ServiceReferrals"
  WHERE
    "deletedAt" IS NULL
    AND "isCurrentVersion"
    AND "formVersion" = 'Ingested'
), starts AS (
  SELECT
	"ServiceReferralId",
    MAX(srts.id) start_id
  FROM
	replica."ServiceReferralTimelineStages" srts
  INNER JOIN
    replica."StageTypes" st
  	ON "StageTypeId" = st.id
  WHERE
    srts."deletedAt" IS NULL
  	AND "defaultOrder" = 1
   	AND NOT "isEdit"
  GROUP BY
    "ServiceReferralId"
), reopened AS (
  SELECT
    id "ServiceReferralId",
    true reopened
  FROM
    referrals
  WHERE
    "referralState" = 'Reopened'
), stages AS (
  SELECT DISTINCT ON ("ServiceReferralId")
    "ServiceReferralId" id,
    date,
    name
  FROM
    replica."ServiceReferralTimelineStages" srts
  INNER JOIN
    replica."StageTypes" st
  	ON "StageTypeId" = st.id
  LEFT OUTER JOIN
    starts
  USING("ServiceReferralId")
  LEFT OUTER JOIN
    reopened
  USING("ServiceReferralId")
  WHERE
    srts."deletedAt" IS NULL
    AND name = 'Resolved'
    AND (srts.id >= start_id OR start_id IS NULL)
    AND NOT (NOT reopened IS NULL AND reopened AND name = 'Resolved')
    AND "ServiceReferralId" IN (SELECT id FROM referrals)
), manual_resolutions AS (
	SELECT
	id,
  	date
	FROM stages
	WHERE name = 'Resolved' 
), auto_enddate_resolutions AS (
	SELECT
  	id,
  	"endDate" date
	FROM referrals 
), auto_startdate_resolutions AS (
	SELECT
  	id,
  	coalesce("startDate" + interval '180 days', "createdAt" + interval '180 days') date
	FROM referrals 
), union_resolutions AS (
	SELECT
  	id,
  	date
	FROM manual_resolutions
	UNION 
	SELECT
  	id,
  	date
	FROM auto_enddate_resolutions
	UNION 
	SELECT
  	id,
  	date
	FROM auto_startdate_resolutions
), resolutions AS (
	SELECT 
	id,
	min(date) date
	FROM union_resolutions
	GROUP BY id
), referral_resolutions AS (
	SELECT
	referrals.id,
	"startDate",
	"endDate",
	"createdAt",
	"routingOrganizationId",
	date dt_resolves
	FROM referrals
	INNER JOIN resolutions
	ON referrals.id = resolutions.id
), emergent_orgs AS (
	SELECT 
	destination_org_id
	FROM replica.referral_routes
	WHERE routing_field_value LIKE '%Emergent 72-hour initial visit%'
), accepted_referrals AS (
	SELECT DISTINCT
	"ServiceReferralId",
	MIN("createdAt") OVER(PARTITION BY "ServiceReferralId") dt_first_accepted,
	1 fl_accepted
	FROM replica."ServiceReferralTimelineStages"
	WHERE "StageTypeId" = 8
), dcyf_orgs AS (
	SELECT o.id 
	FROM replica."Organizations" o
    JOIN replica."OrganizationContracts" oc 
	ON o.id = oc."contractedOrganizationId"
	AND oc."contractOwnerId" = 21
), child_referral_versions AS (
	SELECT *,
	CASE WHEN fl_accepted IS NULL THEN 0 ELSE fl_accepted END AS fl_prov_accepted,
	CASE WHEN (routing_organization_id IN (SELECT destination_org_id FROM emergent_orgs) 
			   AND initial_visit_outcome != 'Not a 72 Hour Referral') THEN 1 ELSE 0 END AS fl_emergent,
	CAST(opd AS DATE) dt_opd
	FROM (
		SELECT
		id,
		"createdAt" dt_ingested,
		"updatedAt" dt_updated,
		"startDate" dt_start,
		"isCurrentVersion" is_current_version,
		"routingOrganizationId" routing_organization_id,
		child_details->>'childFamlinkPersonID' id_child,
		child_details->>'childFirstName' first_name,
		child_details->>'childLastName' last_name,
		child_details->>'childOpd' opd,
		child_details->>'initialVisitOutcome' initial_visit_outcome,
		MAX(CASE WHEN "deletedAt" IS NOT NULL THEN 1 ELSE 0 END) OVER (PARTITION BY id) deleted
		FROM replica."ServiceReferrals",
		json_array_elements("childDetails") child_details
		WHERE "formVersion" = 'Ingested'
		AND "organizationId" IN (SELECT id FROM dcyf_orgs)) referral_versions
		LEFT OUTER JOIN accepted_referrals
		ON referral_versions.id = accepted_referrals."ServiceReferralId"
	WHERE deleted = 0
	AND id_child IS NOT NULL
), opd_max_ref_version AS ( /* current version of referral with the max version opd */
	SELECT 
	child_referral_versions.id,
	child_referral_versions.id_child,
	first_name,
	last_name,
	dt_ingested,
	fl_prov_accepted,
	dt_first_accepted,
	dt_start,
	fl_emergent,
	dt_opd,
	/* if dt_opd is NULL and max_version_opd is not, use max_version_opd */
	CASE WHEN dt_opd IS NULL THEN max_version_opd ELSE dt_opd END AS dt_opd_imputed1,
	MIN(child_referral_versions.id) OVER(PARTITION BY child_referral_versions.id_child) first_referral_id,
	MAX(child_referral_versions.id) OVER(PARTITION BY child_referral_versions.id_child) last_referral_id,
	initial_visit_outcome
	FROM child_referral_versions 
	LEFT OUTER JOIN (
		SELECT DISTINCT
		id,
		id_child,
		dt_opd max_version_opd
		FROM(
			SELECT *,
			/* get the last entered opd in referral versions if there is one */
			MAX(dt_updated) OVER (PARTITION BY id, id_child) max_dt_updated
			FROM child_referral_versions
			WHERE dt_opd IS NOT NULL) max_updates
		WHERE dt_updated = max_dt_updated) omrv
	ON child_referral_versions.id = omrv.id
	AND child_referral_versions.id_child = omrv.id_child
	WHERE is_current_version
), opd_list AS ( /* get list of FIRST non-emergent referrals for unique child id/opd group to use to create episode intervals */
	SELECT *
	FROM (
		SELECT
		id_child,
		id,
		/* anchored to first referral for episode */
		MIN(id) OVER (PARTITION BY id_child, dt_opd_imputed1) min_ref_id,
		dt_opd_imputed1,
		last_referral_id
		FROM opd_max_ref_version
		WHERE dt_opd_imputed1 IS NOT NULL
		/* created intervals off of non-emergent referrals only */
		AND fl_emergent = 0) omr
	WHERE id = min_ref_id
	ORDER BY id_child, min_ref_id
),  opd_windows AS ( /* use opd_list to create start and end episode dates */
	SELECT 
	id_child,
	min_dt_opd_imputed1,
	id_ep_start,
	CASE WHEN (id_ep_end IS NULL) THEN last_referral_id
	ELSE id_ep_end END AS id_ep_end
	FROM(
		SELECT
		id_child,
		dt_opd_imputed1 min_dt_opd_imputed1,
		last_referral_id,
		/* min_ref_id as start */
		min_ref_id id_ep_start,
		/* next child opd minus one day as end */
		LEAD(min_ref_id, 1) OVER (PARTITION BY id_child ORDER BY min_ref_id) - 1 id_ep_end
		FROM opd_list) ol
), child_referrals_non_emergent AS (
	SELECT DISTINCT *,
	/* if coalesced opd is still missing, use the start date*/
	CASE WHEN (dt_opd_imputed2 IS NULL AND fl_emergent = 0) THEN (dt_start)
	ELSE dt_opd_imputed2 END AS dt_opd_imputed3
	FROM (
		SELECT *,
		ROW_NUMBER() OVER(PARTITION BY id, id_child ORDER BY dt_opd_imputed2) row_num	
	FROM (
		SELECT omrv.id_child,
		first_name,
		last_name,
		id,
		dt_start,
		fl_emergent,
		fl_prov_accepted,
		dt_first_accepted,
		dt_opd,
		dt_opd_imputed1,
		/* if child opd is missing and not a 72hr referral then impute previous referral's opd if one exists and 
		if start date is after or on that opd */
		CASE WHEN dt_opd_imputed1 IS NULL
		AND fl_emergent = 0
		AND id > id_ep_start
		AND id <= id_ep_end  THEN min_dt_opd_imputed1
		ELSE dt_opd_imputed1 END AS dt_opd_imputed2,
		first_referral_id,
		initial_visit_outcome
		FROM (
			SELECT *
			FROM opd_max_ref_version) omrv
		LEFT OUTER JOIN (
			SELECT *
			FROM opd_windows) ow
		ON omrv.id_child = ow.id_child) oo ) ooo
	WHERE row_num = 1
), opd_list2 AS ( /* get list of LAST non-emergent referrals for unique child id/opd group to use to create episode intervals */
	SELECT *
	FROM (
		SELECT
		id_child,
		id,
		/* anchor to last referral for episode */
		MAX(id) OVER (PARTITION BY id_child, dt_opd_imputed3) max_ref_id,
		dt_opd_imputed3 max_dt_opd_imputed3,
		first_referral_id
		FROM child_referrals_non_emergent
		WHERE dt_opd_imputed3 IS NOT NULL
		AND fl_emergent = 0) omr
	WHERE id = max_ref_id
	ORDER BY id_child, max_ref_id
), opd_windows2 AS ( /*use opd_list to create start and end id intervals*/
	SELECT 
	id_child,
	max_dt_opd_imputed3,
	CASE WHEN (id_ep_start IS NULL) THEN first_referral_id
	ELSE id_ep_start END AS id_ep_start,
	id_ep_end
	FROM(
		SELECT
		id_child,
		first_referral_id,
		max_dt_opd_imputed3,
		/* previous max ref id plus one as start because we don't want to use dt_start as a reference */
		/* we care more about the order in which the referral came in */
		LAG(max_ref_id, 1, NULL) OVER (PARTITION BY id_child ORDER BY max_ref_id) + 1 id_ep_start,
		/* max_ref_id as end */
		max_ref_id id_ep_end
		FROM opd_list2) ol
), child_referrals_emergent AS (
	SELECT DISTINCT *, 
	CASE WHEN (dt_opd_imputed5 IS NULL AND fl_emergent = 1) THEN (dt_start)
	ELSE dt_opd_imputed5 END AS dt_opd_imputed6
	FROM (
		SELECT *,
		ROW_NUMBER() OVER(PARTITION BY id, id_child ORDER BY dt_opd_imputed4) row_num	
		FROM (
		SELECT *,
		/* if coalesced opd is still missing, use the start date */
		CASE WHEN (dt_opd_imputed4 IS NULL AND fl_emergent = 1) THEN (dt_opd)
		ELSE dt_opd_imputed4 END AS dt_opd_imputed5
			FROM (
			SELECT crne.id_child,
			first_name,
			last_name,
			id,
			fl_prov_accepted,
			dt_first_accepted,
			dt_start,
			fl_emergent,
			dt_opd,
			dt_opd_imputed1,
		dt_opd_imputed2,
		dt_opd_imputed3,
		/* if child opd is missing and not a 72hr referral then impute a previous referral's opd if one exists and 
		if start date is after or on that opd */
		CASE WHEN fl_emergent = 1
		AND id >= id_ep_start
		AND id < id_ep_end  THEN max_dt_opd_imputed3
		ELSE dt_opd_imputed3 END AS dt_opd_imputed4,
		initial_visit_outcome
			FROM (
			SELECT *
			FROM child_referrals_non_emergent) crne
		LEFT OUTER JOIN (
			SELECT *
			FROM opd_windows2) ow
		ON crne.id_child = ow.id_child) oo) ooo) oooo
	WHERE row_num = 1
), child_referral_episodes AS (
	SELECT id_child,
	first_name,
	last_name,
	id,
	fl_prov_accepted,
	dt_first_accepted,
	dt_start,
	fl_emergent,
	dt_opd, 
	dt_opd_imputed6 dt_opd_coalesced,
	to_char(dt_opd_imputed6, 'YYYYMMDD')::int dt_opd_int,
	initial_visit_outcome
	FROM child_referrals_emergent
	ORDER BY id_child, id
), visit_reports AS (
	SELECT *
	FROM (
		SELECT DISTINCT
		"serviceReferralId" id_referral,
		id id_visit,
		date dt_visit,
		virtual,
		"reportType" report_type,
		visit_attendees ->> 'attendeeFirstName' child_first_name,
		visit_attendees ->> 'attendeeLastName' child_last_name,
		CASE WHEN "reportType" IN ('Parent', 'Sibling') THEN ('Attended')
		ELSE "reportType" END AS attendance_status 
		FROM replica."VisitReports",
		json_array_elements("visitAttendees") visit_attendees
		WHERE "isCurrentVersion"
		AND "deletedAt" is NULL
		AND state = 'approved') vr
		INNER JOIN (
			SELECT DISTINCT
			id
			FROM child_referral_episodes) filt
			ON vr.id_referral = filt.id
), visit_referrals AS (
 	SELECT 
	visit_reports.id_referral,
	visit_reports.child_first_name,
	visit_reports.child_last_name,
 	id_visit,
 	id_child,
 	dt_visit,
 	virtual,
 	CASE WHEN report_type IN ('Parent', 'Sibling') THEN ('Attended')
 	ELSE report_type END AS attendance_status 
 	FROM visit_reports
 	LEFT OUTER JOIN (
 		SELECT DISTINCT
 		id,
 		id_child,
 		first_name,
 		last_name
 		FROM child_referral_episodes) cre
 	ON visit_reports.id_referral = cre.id
 	AND visit_reports.child_first_name = cre.first_name
 	AND visit_reports.child_last_name = cre.last_name
	WHERE id_child IS NOT NULL
), first_report AS (
	SELECT 
	id_referral,
	id_child,
	id_visit id_first_report,
	dt_visit dt_first_report,
	attendance_status
	FROM (
		SELECT *,
		ROW_NUMBER() OVER(PARTITION BY id_referral, id_child ORDER BY id_visit) row_num
		FROM visit_referrals) first_reports
	WHERE row_num = 1
), first_attended_visit AS (
	SELECT
	id_referral,
	id_child,
	id_visit id_first_visit,
	dt_visit dt_first_visit,
	CASE WHEN (virtual) THEN (1)
	ELSE 0
	END AS cd_first_visit_modality,
	CASE WHEN (virtual) THEN ('Virtual')
	ELSE 'In Person'
	END AS first_visit_modality
	FROM (
		SELECT *,
		ROW_NUMBER() OVER(PARTITION BY id_child, id_referral ORDER BY id_visit) row_num
		FROM visit_referrals 
		WHERE attendance_status = 'Attended') first_attended_visits
	WHERE row_num = 1
), visit_report_tbl AS (
	SELECT
	CASE WHEN first_report.id_referral IS NULL THEN first_attended_visit.id_referral
	ELSE first_report.id_referral END AS id_referral,
	CASE WHEN first_report.id_child IS NULL THEN first_attended_visit.id_child
	ELSE first_report.id_child END AS id_child,
	id_first_report,
	dt_first_report,
	attendance_status first_report_status,
	id_first_visit,
	dt_first_visit,
	cd_first_visit_modality,
	first_visit_modality
	FROM first_report
	FULL OUTER JOIN first_attended_visit
	ON first_report.id_referral = first_attended_visit.id_referral
	AND first_report.id_child = first_attended_visit.id_child
), child_visitation_tbl AS (
	SELECT 
	CONCAT_WS('_', child_referral_episodes.id_child, child_referral_episodes.id) AS "ID_Child_Referral",
	CONCAT_WS('_', child_referral_episodes.id_child, dt_opd_int) AS "ID_Child_Removal_Episode",
	child_referral_episodes.id "ID_Visitation_Referral",
	CAST(child_referral_episodes.id_child AS int) "ID_Person",
	dt_opd "DT_OPD",
	CASE WHEN dt_opd IS NULL AND dt_opd_coalesced IS NOT NULL THEN dt_opd_coalesced ELSE NULL END AS "DT_OPD_Imputed",
	CASE WHEN dt_opd IS NULL AND dt_opd_coalesced IS NOT NULL THEN 1::smallint ELSE 0::smallint END AS "FL_Imputed",
	dt_start "DT_Start",
	dt_opd_coalesced "DT_OPD_Coalesced",
	dt_resolves::date "DT_Referral_Resolved",
	dt_first_accepted::date "DT_First_Accepted",
	id_first_report "ID_First_Report",
	dt_first_report "DT_First_Report",
	first_report_status "First_Report_Status",
	id_first_visit "ID_First_Attended_Visit",
	dt_first_visit "DT_First_Attended_Visit",
	cd_first_visit_modality::smallint "CD_First_Visit_Modality",
	first_visit_modality::varchar "First_Visit_Modality",
	CASE WHEN dt_resolves <= CURRENT_TIMESTAMP THEN 1::smallint
	ELSE 0::smallint END AS "FL_Visitation_Ended",
	fl_emergent::smallint "FL_Referral_72_Hour",
	initial_visit_outcome::varchar "Outcome_72_Hour_Visit"
	FROM child_referral_episodes
	LEFT OUTER JOIN visit_report_tbl
	ON child_referral_episodes.id_child = visit_report_tbl.id_child
	AND child_referral_episodes.id = visit_report_tbl.id_referral
	lEFT OUTER JOIN referral_resolutions
	ON child_referral_episodes.id = referral_resolutions.id
)
SELECT *,
now() "DT_View_Refreshed"
FROM child_visitation_tbl
ORDER BY "ID_Visitation_Referral", "ID_Person";