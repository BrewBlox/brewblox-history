#! /bin/bash

function create_policy() {
    docker-compose exec influx influx -execute \
    " \
    CREATE RETENTION POLICY $1 ON brewblox DURATION $2 REPLICATION 1 SHARD DURATION $3; \
    ALTER RETENTION POLICY $1 ON brewblox DURATION $2 REPLICATION 1 SHARD DURATION $3; \
    "
}

function create_query() {
    docker-compose exec influx influx -execute \
    " \
    DROP CONTINUOUS QUERY cq_downsample_$1 ON brewblox; \
    CREATE CONTINUOUS QUERY cq_downsample_$1 ON brewblox \
    BEGIN \
        SELECT mean(*) AS m INTO brewblox.downsample_$1.:MEASUREMENT \
        FROM brewblox.$2./.*/ \
        GROUP BY time($1),* \
    END \
    "
}

docker-compose exec influx influx -execute 'CREATE DATABASE brewblox'

create_policy "autogen" "1d" "6h"
create_policy "downsample_1m" "INF" "1w"
create_policy "downsample_10m" "INF" "1w"
create_policy "downsample_1h" "INF" "1w"
create_policy "downsample_6h" "INF" "1w"

create_query "1m" "autogen"
create_query "10m" "downsample_1m"
create_query "1h" "downsample_10m"
create_query "6h" "downsample_1h"


docker-compose exec influx influx -execute 'show databases'
docker-compose exec influx influx -execute 'show retention policies on brewblox'
docker-compose exec influx influx -execute 'show continuous queries'
