DROP MATERIALIZED VIEW IF EXISTS dcyf.transport_detail CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS dcyf.transport_detail AS

SELECT vr.id "ID_Visit",
sr.id "ID_Visitation_Referral",
"visitPlanId" "ID_Visit_Plan",
"organizationId" "ID_Provider_Sprout",
direction "Direction_Of_Transport",
starttime "Start_Time",
endtime "End_Time",
regexp_replace(distance, '[a-zA-Z]', '') "Transport_Distance",
vr.driver_id "ID_Driver",
driver_name "Driver_Name",
child_name "Children_On_Transport",
now() "DT_View_Refreshed"
FROM (
SELECT id,
"serviceReferralId",
transport_details ->> 'directionOfTransport' direction,
transport_details ->> 'transportStartTime' starttime,
transport_details ->> 'transportEndTime' endtime,
transport_details ->> 'transportDistance' distance,
(NULLIF((transport_details ->> 'driver'), ''))::int driver_id,
transport_details ->> 'child' child_name
FROM dcyf.visit_reports,
json_array_elements("transportDetails") transport_details
WHERE "isCurrentVersion"
AND "deletedAt" IS NULL) vr
INNER JOIN (SELECT id,
			"visitPlanId",
			"organizationId"
			FROM dcyf.service_referrals
			WHERE "deletedAt" IS NULL
			AND "isCurrentVersion" 
			AND "formVersion" = 'Ingested') sr
ON vr."serviceReferralId" = sr.id
LEFT JOIN (SELECT id driver_id,
		   "firstName" || ' ' || "lastName" driver_name
		   FROM replica."Users") u
ON vr.driver_id = u.driver_id