A simple flink job that runs a sql script

# Flags 

* --sql - The sql script to execute
* --hive-conf - The directory containing hive-site.xml
* --hive-version - The hive version, default 1.1.0

# Usage

```bash
./bin/flink run -p 10 -m yarn-cluster flink-sql-example.jar --sql queries.sql --hive-conf /etc/hive/conf
```
