SELECT o.id AS sprout_provider_id,
	name AS provider_name,
	"contactName" AS provider_contact_name,
	phone AS provider_contact_phone,
	email AS provider_contact_email,
	"billingAddress" AS billing_address_line1,
	city AS billing_address_city,
	"state" AS billing_address_state,
	"postalCode" AS billing_address_postal_code,
	"famlinkId" AS famLink_provider_id,
	"visitationOnSaturday" AS FL_Visitation_On_Saturday,
	"visitationOnSunday" AS FL_Visitation_On_Sunday,
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
	searchable AS fl_searchable,
	"stateServiced" AS State_Serviced,
	"routingOrg" AS FL_Routing_Organization,
	"hasSocialWorkers" AS FL_Has_Social_Workers,
	o."createdAt" AS dt_create,
	o."updatedAt" AS dt_update,
	o."deletedAt" AS dt_deleted
	-- Counties_Serviced
FROM staging."Organizations" AS o
JOIN staging."OrganizationContracts" AS oc 
    ON o.id = oc."contractedOrganizationId"
	AND oc."contractOwnerId" = 21
ORDER BY o.id