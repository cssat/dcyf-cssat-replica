WITH counties AS (
	SELECT "organizationId", 
		"countyId" AS cd_county,
		co."name" AS county
	FROM replica."OrganizationCounties" AS oc
	JOIN replica."Counties" AS co
		ON oc."countyId" = co.id
)
SELECT o.id AS id_provider_sprout,
	cd_county,
	county
FROM dcyf."organizations" AS o
INNER JOIN counties AS co
	ON o.id = co."organizationId"
ORDER BY o.id,
	cd_county
