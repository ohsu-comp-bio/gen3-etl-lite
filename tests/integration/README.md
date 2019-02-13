# integration tests


* These tests are designed to run `outside` the etl-service container




```
# from host machine

$ docker-compose ps
           Name                         Command               State                       Ports
--------------------------------------------------------------------------------------------------------------------
composeservices_postgres_1   docker-entrypoint.sh postg ...   Up      0.0.0.0:5432->5432/tcp
esproxy-service              /usr/local/bin/docker-entr ...   Up      0.0.0.0:9200->9200/tcp, 0.0.0.0:9300->9300/tcp
etl-service                  /bin/sh /gen3_replicator/d ...   Up
fence-service                /entrypoint bash /var/www/ ...   Up      22/tcp, 80/tcp
indexd-service               bash indexd_setup.sh             Up      80/tcp
kibana-service               /usr/local/bin/kibana-docker     Up      0.0.0.0:5601->5601/tcp
peregrine-service            /bin/sh /peregrine/dockerr ...   Up      80/tcp
portal-service               bash /var/www/data-portal/ ...   Up
revproxy-service             nginx -g daemon off;             Up      0.0.0.0:443->443/tcp, 0.0.0.0:80->80/tcp
sheepdog-service             bash /sheepdog_setup.sh          Up      80/tcp

$ python -m pytest tests
```
