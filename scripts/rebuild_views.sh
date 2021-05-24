#!/bin/bash

: '
Start a timer for the rebuild process, set
variables for project paths, and source the
.env file for the project.
'

start=$(date +%s)
full_path=$(realpath $0)
dir_path=$(dirname $full_path)
root=$(dirname $dir_path )
source $root/.env
echo -e "\033[32m.env source - attempt complete\033[m"

: '
Clear away the old schemas and all dependent relations,
then set baseline configurations.
'

PGPASSWORD=$CSSAT_DCYF_SPROUT_REPLICA_PWD psql -h localhost -p 5433 -U postgres -d sprout_replica -a -f "$dir_path/drop_schema_relations.sql"
echo -e "\033[32mclear config - attempt complete\033[m"
PGPASSWORD=$CSSAT_DCYF_SPROUT_REPLICA_PWD psql \
-v sprout_db_name="$SPROUT_DB_NAME" \
-v sprout_db_port="$SPROUT_DB_PORT" \
-v sprout_db_server="$SPROUT_DB_SERVER" \
-v sprout_db_user="$SPROUT_DB_USER" \
-v sprout_db_pwd="$SPROUT_DB_PWD" \
-h localhost -p 5433 -U postgres -d sprout_replica -a -f "$root/schema/sprout/sprout_fdw_config.sql"
echo -e "\033[32mfdw config - attempt complete\033[m"

: '
Rebuild all views in the database. For several of the views,
order does matter due to dependencies between the views.
'

PGPASSWORD=$CSSAT_DCYF_SPROUT_REPLICA_PWD psql -h localhost -p 5433 -U postgres -d sprout_replica -a -f "$root/schema/curated/Organizations_mv.sql"
echo -e "\033[32morganizations build - attempt complete\033[m"
PGPASSWORD=$CSSAT_DCYF_SPROUT_REPLICA_PWD psql -h localhost -p 5433 -U postgres -d sprout_replica -a -f "$root/schema/curated/OrganizationLocations_mv.sql"
echo -e "\033[32morganization locations build - attempt complete\033[m"
PGPASSWORD=$CSSAT_DCYF_SPROUT_REPLICA_PWD psql -h localhost -p 5433 -U postgres -d sprout_replica -a -f "$root/schema/curated/ServiceReferrals_mv.sql"
echo -e "\033[32mservice referrals build - attempt complete\033[m"
PGPASSWORD=$CSSAT_DCYF_SPROUT_REPLICA_PWD psql -h localhost -p 5433 -U postgres -d sprout_replica -a -f "$root/schema/curated/ServiceReferralIdsNonDCYF_mv.sql"
echo -e "\033[32mservice referral ids non dcyf build - attempt complete\033[m"
PGPASSWORD=$CSSAT_DCYF_SPROUT_REPLICA_PWD psql -h localhost -p 5433 -U postgres -d sprout_replica -a -f "$root/schema/curated/VisitReports_mv.sql"
echo -e "\033[32mvisit reports build - attempt complete\033[m"
PGPASSWORD=$CSSAT_DCYF_SPROUT_REPLICA_PWD psql -h localhost -p 5433 -U postgres -d sprout_replica -a -f "$root/schema/curated/ServiceReferralTimelineStages_mv.sql"
echo -e "\033[32mtimeline stages build - attempt complete\033[m"
PGPASSWORD=$CSSAT_DCYF_SPROUT_REPLICA_PWD psql -h localhost -p 5433 -U postgres -d sprout_replica -a -f "$root/schema/curated/StageTypes_mv.sql"
echo -e "\033[32mtimeline stage types build - attempt complete\033[m"
PGPASSWORD=$CSSAT_DCYF_SPROUT_REPLICA_PWD psql -h localhost -p 5433 -U postgres -d sprout_replica -a -f "$root/schema/curated/UnusualIncidentReports_mv.sql"
echo -e "\033[32munusual incident reports build - attempt complete\033[m"
PGPASSWORD=$CSSAT_DCYF_SPROUT_REPLICA_PWD psql -h localhost -p 5433 -U postgres -d sprout_replica -a -f "$root/schema/curated/UnusualIncidentInvolvedParties_mv.sql"
echo -e "\033[32munusual incident parties build - attempt complete\033[m"
PGPASSWORD=$CSSAT_DCYF_SPROUT_REPLICA_PWD psql -h localhost -p 5433 -U postgres -d sprout_replica -a -f "$root/schema/curated/UnusualIncidentInvolvedPartyTypes_mv.sql"
echo -e "\033[32munusual incident party types build - attempt complete\033[m"
PGPASSWORD=$CSSAT_DCYF_SPROUT_REPLICA_PWD psql -h localhost -p 5433 -U postgres -d sprout_replica -a -f "$root/schema/curated/UnusualIncidentReportActions_mv.sql"
echo -e "\033[32munusual incident actions build - attempt complete\033[m"
PGPASSWORD=$CSSAT_DCYF_SPROUT_REPLICA_PWD psql -h localhost -p 5433 -U postgres -d sprout_replica -a -f "$root/schema/curated/UnusualIncidentActionTypes_mv.sql"
echo -e "\033[32munusual incident action types build - attempt complete\033[m"

: '
Grant privs to dcyf-replica-user and set up pg_cron
schedule to refresh the views every night.
'

PGPASSWORD=$CSSAT_DCYF_SPROUT_REPLICA_PWD psql -h localhost -p 5433 -U postgres -d sprout_replica -a -f "$dir_path/dcyf_grants.sql"
echo -e "\033[32mgrant access to dcyf-replica-user - attempt complete\033[m"
PGPASSWORD=$CSSAT_DCYF_SPROUT_REPLICA_PWD psql -h localhost -p 5433 -U postgres -d postgres -a -f "$dir_path/schedule_refresh.sql"
echo -e "\033[32mset refresh cron - attempt complete\033[m"

: '
End the timer and dump total
processing time.
'

end=$(date +%s)

seconds=$(echo "$end - $start" | bc)
echo $seconds' sec'

echo 'Total Duration:'
awk -v t=$seconds 'BEGIN{t=int(t*1000); printf "%d:%02d:%02d\n", t/3600000, t/60000%60, t/1000%60}'
