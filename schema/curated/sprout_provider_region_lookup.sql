SELECT o.id AS sprout_provider_id,
	"regionId" AS cd_region,
	CONCAT('Region ', "regionId") AS region,
	NULL AS cd_office,
	NULL AS office
FROM staging."Organizations" AS o
JOIN staging."OrganizationContracts" AS oc 
    ON o.id = oc."contractedOrganizationId"
	AND oc."contractOwnerId" = 21
ORDER BY o.id