## Prepare data and binary
* in sql dir
```
./integ.sh localhost:9200 integ-test/src/test/resources/indexDefinitions/account_index_mapping.json integ-test/src/test/resources/accounts.json

./deploy.sh
```
## Launch Livy + Spark local mode 
* in livy dir
```
./bin/livy-server start
```

## Launch OpenSearch
* in opensearch 2.17 folder
```
./bin/opensearch
```

## Test query
```
###
POST {{baseUrl}}/_plugins/_sql/
Content-Type: application/x-ndjson

{
  "query": "SELECT t1.address as t1a, t2.address as t2a FROM dev.default.my_index as t1 INNER JOIN dev.default.my_index as t2 ON t1.address=t2.address LIMIT 10"
}    
```