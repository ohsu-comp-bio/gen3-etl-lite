# gen3-etl-lite
A lightweight ETL from gen3 metadata_db to elastic search index.

## use case

In the current  [compose-services](https://github.com/uc-cdis/compose-services), the `exploration` tab is broken.
![image](https://user-images.githubusercontent.com/47808/51440706-bf276d80-1c7e-11e9-952f-d22de826cea4.png)

A functional explorer page is necessary to demo private data on a behind the firewall server.  This the replicator and accompanying UI plugin enables exploration for evaluation and iteration.

While a [docker-compose](https://github.com/uc-cdis/compose-services/blob/master/etl/docker-compose.yml) for extraction and search exists, the [etl component](https://github.com/uc-cdis/tube) depends on a relatively heavy weight dependency [spark, hadoop].  These dependencies are implemented primarily on aws and google.  They are only partially implemented on native linux.  The extract is tied to a  UI component [arranger](https://github.com/uc-cdis/gen3-arranger) incorporating the services from OICR's [overture-stack](https://github.com/overture-stack/arranger).

At this time the provided etl and faceted search are not supported in the lightweight compose-services deployment.

This repository provides a lightweight service to observe the changes to the metadata_db database and produce a flattened search index in elastic search.

## dependencies

The postgres service used by the gen3 instance is configured with:
  * replication slots
  * replication user
  * the [wal2json](https://github.com/eulerto/wal2json) postgres plugin.
These changes are maintained on this fork/branch of [compose-services](TODO)


Temporarily, until the arranger service is better supported, the (data-portal explorer page)[https://github.com/ohsu-comp-bio/data-portal/blob/ohsu-etl/src/DataExplorer/index.jsx#L5-L13] has been modified to simply display a kibana dashboard.

## design

![image](https://user-images.githubusercontent.com/47808/51441175-aa99a400-1c83-11e9-8c1a-e34865e357e8.png)
