CREATE MATERIALIZED VIEW IF NOT EXISTS curated."StageTypes_mv" AS

SELECT  
    "id",
    "createdAt",
    "updatedAt",
    "deletedAt",   
    "name",
    "defaultOrder",
    "label", 
    "editableByProvider",
    "visibleToProvider",
    "editableByContractOwner",
    now() "viewRefreshedAt"
FROM sprout."StageTypes";