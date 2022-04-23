WITH offices1 AS (
	SELECT DISTINCT "organizationId", "dshsOffice"
	FROM dcyf."service_referrals" AS sr
	WHERE  sr."isCurrentVersion"
		AND "formVersion" = 'Ingested'
),
offices AS (
  SELECT DISTINCT 
	"organizationId",
	SUBSTRING("dshsOffice",'\(([0-9].*)\)')::INT AS cd_office,	
		"dshsOffice" AS office
	FROM offices1
)
SELECT o.id AS id_provider_sprout,
	cd_office,
	office
FROM dcyf."organizations" AS o
INNER JOIN offices AS offi
	ON o.id = offi."organizationId"
ORDER BY id_provider_sprout,
	cd_office
