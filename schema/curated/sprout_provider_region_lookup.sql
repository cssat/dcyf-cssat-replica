WITH org_region AS (
	SELECT DISTINCT "regionId", 
		"organizationId"
	FROM staging."ServiceReferrals" AS sr
	WHERE sr."deletedAt" IS NULL
		AND sr."isCurrentVersion"
		AND "formVersion" = 'Ingested'
)
SELECT o.id AS sprout_provider_id,
	COALESCE(org_reg."regionId", o."regionId") AS cd_region,
	CONCAT('Region ', COALESCE(org_reg."regionId", o."regionId")) AS region
FROM staging."Organizations" AS o
JOIN staging."OrganizationContracts" AS oc 
    ON o.id = oc."contractedOrganizationId"
	AND oc."contractOwnerId" = 21
LEFT JOIN org_region AS org_reg 
	ON o.id = org_reg."organizationId"
WHERE org_reg."regionId" != 7
ORDER BY o.id,
	cd_region