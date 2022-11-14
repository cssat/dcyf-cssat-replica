DROP MATERIALIZED VIEW IF EXISTS dcyf.transport_detail;
CREATE MATERIALIZED VIEW IF NOT EXISTS dcyf.transport_detail AS

	SELECT id::varchar || leg::varchar "ID_Visit_Transport",
	id "ID_Visit",
	"serviceReferralId" "ID_Visitation_Referral",
	"visitPlanId" "ID_Visit_Plan",
	"organizationId" "ID_Provider_Sprout",
	direction "Direction_Of_Transport",
	visit_date "DT_Visit",
	"DT_Transport_Start",
	"DT_Transport_Stop",
	(NULLIF(regexp_replace(transport_distance, '[^0-9.]', '', 'g'), ''))::float "Transport_Distance",
	driver_id "ID_Driver",
	driver_name "Driver_Name",
	child_name "Children_On_Transport",
	now() "DT_View_Refreshed"
	FROM (
		SELECT *,
		TO_TIMESTAMP(visit_date::text || ' ' || tst2::text, 'YYYY-MM-DD HH12:MI AM') "DT_Transport_Start", 
		TO_TIMESTAMP(visit_date::text || ' ' || tet2::text, 'YYYY-MM-DD HH12:MI AM') "DT_Transport_Stop"
		FROM (
			SELECT *,
			REPLACE(REPLACE(tet, 'pm', 'p'), 'p', 'PM') AS tet2,
			REPLACE(REPLACE(tst, 'pm', 'p'), 'p', 'PM') AS tst2
			FROM (
				SELECT *,
				REPLACE(REPLACE(LOWER(transport_end_time), 'am', 'a'), 'a', 'AM') AS tet,
				REPLACE(REPLACE(LOWER(transport_start_time), 'am', 'a'), 'a', 'AM') AS tst,
				REGEXP_REPLACE(transport_distance, '[^.,0-9]',  '', 'g') AS td
				FROM (
					SELECT
					id,
					"serviceReferralId",
					ROW_NUMBER() OVER(PARTITION BY id ORDER BY id) leg,
					date visit_date,
					transport_details ->> 'directionOfTransport' direction,
					CASE WHEN transport_details ->> 'transportEndTime' = '0' THEN NULL 
					ELSE REPLACE(transport_details ->> 'transportEndTime', '00:', '12:') END AS transport_end_time,
					CASE WHEN transport_details ->> 'transportStartTime' = '0' THEN NULL 
					ELSE REPLACE(transport_details ->> 'transportStartTime', '00:', '12:') END AS transport_start_time, 
					CASE WHEN transport_details ->> 'transportDistance' = '' THEN NULL
					ELSE transport_details ->> 'transportDistance' END AS transport_distance,
					(NULLIF((transport_details ->> 'driver'), ''))::int driver_id,
					transport_details ->> 'child' child_name
					FROM dcyf.visit_reports,
					json_array_elements("transportDetails") AS transport_details
					WHERE "isCurrentVersion"
					AND "deletedAt" IS NULL) vr
				INNER JOIN (
					SELECT id referral_id,
					"visitPlanId",
					"organizationId"
					FROM dcyf.service_referrals
					WHERE "deletedAt" IS NULL
					AND "isCurrentVersion" 
					AND "formVersion" = 'Ingested') sr
					ON vr."serviceReferralId" = sr.referral_id
				LEFT JOIN (
					SELECT id user_id,
					"firstName" || ' ' || "lastName" driver_name
					FROM replica."Users") u
				ON vr.driver_id = u.user_id
			) xx
		)xxx 
		)xxxx
