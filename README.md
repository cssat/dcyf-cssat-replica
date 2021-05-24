## Prerequisites

1. Have a technical resource person from CSSAT provide you with the private key for the bastion host for the dcyf-cssat collaboration VPC in AWS. Here, we assume that the file is named `dcyf-cssat-ssh-key.pem`.

2. Have a technical resource person from CSSAT white list your IP address on the VPC. 

3. Make sure you `chmod` the key to ensure that it isn't publicly viewable. 

```
chmod 400 dcyf-cssat-ssh-key.pem
```

4. Have a technical resource person from CSSAT provide you with a username and password for the `sprout_replica` Postgres database. If you are looking to make changes to the curated schema, you will need to be logged in using the `postgres` user. 


## Accessing the database 

### Access via the bastion host

Connect to the bastion server. 

```
ssh -i dcyf-cssat-ssh-key.pem ec2-user@ec2-34-234-208-52.compute-1.amazonaws.com
```

Connect to the database using the `psql` client on the bastion server. 

```
psql postgresql://<User>:<Password>@cssat-dcyf-sprout-replica.crartr7yq7ee.us-east-1.rds.amazonaws.com/sprout_replica
```

### Access via port fowarding
 
 Setup port forwarding

```
 ssh -i "dcyf-replica-user.pem" -f -N -L 5433:cssat-dcyf-sprout-replica.crartr7yq7ee.us-east-1.rds.amazonaws.com:5432 dcyf-replica-user@ec2-34-234-208-52.compute-1.amazonaws.com -v
```

Connect to the database locally using your favorite Postgres client. For example, using a the `psql` terminal client, the following connection string will provide you with access to the replica. 

```
psql postgresql://<User>:<Password>@localhost:5433/sprout_replica
```

## Changing the database

1. Login to aptible and find the credentials for the production sprout database. 

2. Add the aptible credentials to an `.env` file with variables specified as shown below. 

```
CSSAT_DCYF_SPROUT_REPLICA_PWD=<postgres user password>
SPROUT_DB_NAME=<aptible database name>
SPROUT_DB_SERVER=<aptible database server>
SPROUT_DB_PORT=<aptible database port>
SPROUT_DB_USER=<aptible username>
SPROUT_DB_PWD=<aptible password>
```

3. If your new materialized view requires access to a new relation in the production sprout database, edit the schema import in [sprout_fdw_config.sql](schema/sprout/sprout_fdw_config.sql). 

NOTE: If you can write the new materialized view within the existing schema, skip this step and proceed to step 4. 

4. Define the new materialized view (or update an existing view) in the [curated schema](schema/curated).

5. Grant `dcyf-replica-user` access to the materialized view by updating [`dcyf_grants.sql`](scripts/dcyf_grants.sql).

NOTE: If you are just updating an existing materialized view, skip this step and proceed to step 8. 

6. Schedule a nightly refresh of the materialized view by updating [`schedule_refresh.sql`](scripts/schedule_refresh.sql).

7. Update [`rebuild_views.sh`](scripts/rebuild_views.sh) to include a build of your materialized view when the script is run. 

8. Rebuild the materialized views by running 

```
cd scripts
./rebuild_views.sh
```

NOTE: Step 8 accomplishes two things: A. If your materialized view is new, it creates the view so that it can be picked up by the `pg_cron` that you established in Step 6, and B. It helps to make sure that your materialized view contains all of the necessary relation dependencies. 
