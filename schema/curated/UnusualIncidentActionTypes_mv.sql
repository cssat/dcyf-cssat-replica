CREATE MATERIALIZED VIEW IF NOT EXISTS curated."UnusualIncidentActionTypes_mv" AS

SELECT  
    "id",
    "name",
    "defaultOrder",
    "createdAt",
    "updatedAt",
    "deletedAt",
    now() "viewRefreshedAt"
FROM sprout."UnusualIncidentActionTypes";
