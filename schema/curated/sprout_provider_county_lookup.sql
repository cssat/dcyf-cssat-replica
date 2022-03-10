WITH counties AS (
	SELECT "organizationId", 
		"countyId" AS cd_county,
		co."name" AS county
	FROM staging."OrganizationCounties" AS oc
	JOIN staging."Counties" AS co
		ON oc."countyId" = co.id
)
SELECT o.id AS sprout_provider_id,
	cd_county,
	county
FROM staging."Organizations" AS o
JOIN staging."OrganizationContracts" AS oc 
    ON o.id = oc."contractedOrganizationId"
	AND oc."contractOwnerId" = 21
JOIN counties AS co
	ON o.id = co."organizationId"
ORDER BY o.id,
	cd_county