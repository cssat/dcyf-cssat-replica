CREATE MATERIALIZED VIEW IF NOT EXISTS curated."OrganizationLocations_mv" AS

SELECT  
    ol."id",
    ol."organizationId",
    ol."streetAddress",
    ol."city",
    ol."state",
    ol."postalCode",
    ol."createdAt",
    ol."updatedAt",
    ol."deletedAt",
    ol."phone",
    ol."email",
    ol."website",
    ol."primaryLocation",
    ol."operationHoursJson",
    ol."serviceHoursName",
    ol."serviceHoursJson",
    ol."name",
    now() "viewRefreshedAt"
FROM sprout."OrganizationLocations" ol
    JOIN curated."Organizations_mv" o
        ON ol."organizationId" = o.id;

CREATE UNIQUE INDEX IF NOT EXISTS "OrganizationLocations_mv_pkey" 
    ON curated."OrganizationLocations_mv" USING btree ("id");