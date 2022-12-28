# filebeat-clickhouse
The output for filebeat support push events to ClickHouse，You need to recompile filebeat with the ClickHouse output.  
Compared with the combination of KafKa+ZooKeeper+ClickHouse(MATERIALIZED VIEW), it's simpler and less use of system resources in scenarios with small data num.  
Currently compiled based on version `filebeat version 8.6.0, libbeat 8.6.0`.  

## Compile 
Clone beats
```
git clone git@github.com:elastic/beats.git
```
Install filebeat-clickhouse output
```
go get github.com/codytan/filebeat-clickhouse
```
Modify beats outputs includes, add clickhouse output
```
cd {your beats directory}/github.com/elastic/beats/libbeat/publisher/includes;
vim includes.go;

import (
	...
	_ "github.com/codytan/filebeat-clickhouse"
)
```

Build filebeat
```
cd {your beats directory}/github.com/elastic/beats/filebeat
make
```
   
## Output Configure
Config tables: Specify the table and columns. Need to be exactly the same as the field in log file(JSON row format), for compatibility with data types with SQL, the JSON field must be string type.
When clickhouse is not available, failed events can be retried (Filebeat will be retried indefinitely) and other errors will be dropped.

```
filebeat.inputs:
- type: filestream
  id: test-log-id
  enabled: true
  fields: 
    table: "table_name" #clickhouse table name
  paths:
    - ./test.log*
- type: filestream
  id: test-log-id2
  enabled: true
  fields: 
    table: "table_name2" #another table 
  paths:
    - ./test2.log*

output.clickhouse:
  host: ["127.0.0.1:9000"] #clickhouse host
  db: "default" #database
  user_name: "default" 
  pass_word: ""
  batch_size: 1000
  max_retries: -1
  tables: #insert data config, specify the table and columns. Need to be exactly the same as the field in log file(JSON row format), for compatibility with data types with SQL, the JSON field must be string type
    - table: "table_name"
      columns: ["EventTime", "Label.Label", "Label.Count"] #suport nested type
    - table: "table_name2"
      columns: ["id", "name"]

queue.mem:
  events: 4096
  flush.min_events: 1000 #send event num per batch
  flush.timeout: 5s #send event interval
```
