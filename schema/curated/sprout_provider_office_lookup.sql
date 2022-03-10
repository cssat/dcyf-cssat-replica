WITH offices AS (
	SELECT DISTINCT "organizationId", 
		SUBSTRING("dshsOffice",'\(([0-9].*)\)')::INT AS cd_office,	
		"dshsOffice" AS office
	FROM staging."ServiceReferrals" AS sr
	WHERE sr."deletedAt" IS NULL
		AND sr."isCurrentVersion"
		AND "formVersion" = 'Ingested'
)	
SELECT o.id AS sprout_provider_id,
	cd_office,
	office
FROM staging."Organizations" AS o
JOIN staging."OrganizationContracts" AS oc 
    ON o.id = oc."contractedOrganizationId"
	AND oc."contractOwnerId" = 21
LEFT JOIN offices AS offi
	ON o.id = offi."organizationId"
ORDER BY sprout_provider_id,
	cd_office