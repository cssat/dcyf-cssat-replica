WITH cs AS (
	SELECT "organizationId", 
		string_agg("name", ', ') AS counties_serviced
	FROM staging."OrganizationCounties" AS oc
	JOIN staging."Counties" AS c
		ON oc."countyId" = c.id
	GROUP BY "organizationId"
)
SELECT o.id AS sprout_provider_id,
	name AS provider_name,
	"contactName" AS provider_contact_name,
	phone AS provider_contact_phone,
	email AS provider_contact_email,
	"billingAddress" AS billing_address_line1,
	city AS billing_address_city,
	"state" AS billing_address_state,
	"postalCode" AS billing_address_postal_code,
	CASE WHEN regexp_replace(split_part("famlinkId", '/', 1), '\D', '', 'g')~'^([0-9]+)' THEN regexp_replace(split_part("famlinkId", '/', 1), '\D', '', 'g')::INT 
		END AS famLink_provider_id, 
	CASE WHEN "visitationOnSaturday" = 'true' THEN 1
		WHEN "visitationOnSaturday" = 'false' THEN 0
		END AS fl_visitation_on_saturday,
	CASE WHEN "visitationOnSunday" = 'true' THEN 1
		WHEN "visitationOnSunday" = 'false' THEN 0
		END AS fl_visitation_on_sunday,
	"bilingualSupportTypeId" AS bilingual_support_type_id,
	CASE WHEN "bilingualSupportTypeId" = 1 THEN 'No language support offered'
		WHEN "bilingualSupportTypeId" = 1 THEN 'No, use interpreter'
		WHEN "bilingualSupportTypeId" = 1 THEN CONCAT('Yes, specifically these languages: ', "languagesSupported")
		END AS bilingual_support_type, 
	"languagesSupported" AS languages_supported,
	"holidayAvailabilityTypeId" AS holiday_availibility_type_id,
	CASE WHEN "holidayAvailabilityTypeId" = 1 THEN 'Open all holidays (normal hours)'
		WHEN "holidayAvailabilityTypeId" = 2 THEN 'Open all holidays (modified hours)'
		WHEN "holidayAvailabilityTypeId" = 3 THEN 'Open some holidays (call for details)'
		WHEN "holidayAvailabilityTypeId" = 4 THEN CONCAT('Closed only on these holidays: ', "holidaysClosed")
		END AS holiday_availibility_type, 
	"holidaysClosed" AS holiday_closed,
	CASE WHEN searchable = 'true' THEN 1
		WHEN searchable = 'false' THEN 0
		END AS fl_searchable,
	"stateServiced" AS state_serviced,
	CASE WHEN "routingOrg" = 'true' THEN 1
		WHEN "routingOrg" = 'false' THEN 0
		END AS fl_routing_organization,
	CASE WHEN "hasSocialWorkers" = 'true' THEN 1
		WHEN "hasSocialWorkers" = 'false' THEN 0
		END AS fl_has_social_workers,
	o."createdAt" AS dt_create,
	o."updatedAt" AS dt_update,
	o."deletedAt" AS dt_deleted,
	counties_serviced
FROM staging."Organizations" AS o
JOIN staging."OrganizationContracts" AS oc 
    ON o.id = oc."contractedOrganizationId"
	AND oc."contractOwnerId" = 21
JOIN cs 
	ON o.id = cs."organizationId"
ORDER BY o.id