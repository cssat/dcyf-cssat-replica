CREATE SCHEMA IF NOT EXISTS sprout;

SET search_path TO sprout;

CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER IF NOT EXISTS sprout_foreign_prod_db
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (
            host 'sprout_db_server', 
            port 'sprout_db_port', 
            dbname 'sprout_db_name'
        );

CREATE USER MAPPING IF NOT EXISTS FOR postgres
        SERVER sprout_foreign_prod_db
        OPTIONS (
            user 'sprout_db_user', 
            password 'sprout_db_pwd'
        );

IMPORT FOREIGN SCHEMA public 
LIMIT TO (   
     "Organizations", 
     "OrganizationContracts",
     "OrganizationLocations",
     "ServiceReferrals",    
     "ServiceReferralTimelineStages",    
     "StageTypes",     
     "UnusualIncidentInvolvedParties", 
     "UnusualIncidentInvolvedPartyTypes", 
     "UnusualIncidentActionTypes", 
     "UnusualIncidentActions",
     "UnusualIncidentReports",
     "VisitReports"
)
FROM SERVER sprout_foreign_prod_db INTO sprout;

CREATE SCHEMA IF NOT EXISTS curated;
