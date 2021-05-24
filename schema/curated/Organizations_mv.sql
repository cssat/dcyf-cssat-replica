CREATE MATERIALIZED VIEW IF NOT EXISTS curated."Organizations_mv" AS

SELECT  
    o."id",
    o."name",
    o."contactName",
    o."billingAddress",
    o."city",
    o."state",
    o."postalCode",
    o."phone",
    o."email",
    o."createdAt",
    o."updatedAt",
    o."regionId",
    o."orgAdminsCanManageOwnReferrals",
    o."deletedAt",
    o."readOnlyWithinRegion",
    o."callForAccommodations",
    o."famlinkId",
    o."visitationOnSaturday",
    o."visitationOnSunday",
    o."languagesSupported",
    o."bilingualSupportTypeId",
    o."holidaysClosed",
    o."holidayAvailabilityTypeId",
    o."searchable",
    o."stateServiced",
    o."regionState",
    o."routingOrg",
    o."hasSocialWorkers",
    now() "viewRefreshedAt"
FROM sprout."Organizations" o
    JOIN sprout."OrganizationContracts" oc 
        ON o.id = oc."contractedOrganizationId"
            AND oc."contractOwnerId" = 21;

CREATE UNIQUE INDEX IF NOT EXISTS "Organizations_mv_pkey" 
    ON curated."Organizations_mv" USING btree ("id");