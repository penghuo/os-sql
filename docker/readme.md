## Introduction

The docker-compose includes

- OpenSearch 2.5
- OpenSearch Dashboards 2.5
- Spark 3.2.1

## Setup Dev Env

### 0, Prepare test data

```
1. create /tmp/maximus folder which already mount to docker
mkdir -p $HOME/tmp/maximus

2. change working dir
cd $HOME/tmp/maximus

3. create csv file
 ~/tmp/maximus  cat > sample.csv << EOF
id,name,city,age
1,david,Shen Zhen,31
2,eason,Shen Yang,27
3,jarry,Wu Han,35
EOF
```

### 1. build spark image

```
docker build --pull --rm -f "spark/Dockerfile" -t maximus/spark:v3.2.1 "spark" --network host
```

### 2. build OpenSearch image

```
docker build --pull --rm -f "opensearch/Dockerfile" -t maximus/opensearch:2.5.0 "opensearch" --network host
```

### 3. build OpenSearch Dashboards image

```
docker build --pull --rm -f "opensearch-dashboards/Dockerfile" -t maximus/opensearch-dashboards:2.5.0 "opensearch-dashboards" --network host
```

### 4. bootstrap env

```
docker compose -f "docker-compose.yml" up -d --build
```

## Test

### Create table - Only required at first time

```
curl -XPOST 'http://localhost:9200/_plugins/_ppl' \
--header 'Content-Type: application/json' \
--data '{
  "query": "source = myspark.jdbc(\"CREATE TABLE test_table (id INT, name STRING, city STRING, age INT) USING csv OPTIONS (path '/root/maximus/sample.csv', header 'true', mode 'FAILFAST')\")"
}
'
```

### Query table

```
curl -XPOST 'http://localhost:9200/_plugins/_ppl' \
--header 'Content-Type: application/json' \
--data '{
  "query": "source = myspark.jdbc('\''select count(*) from test_table'\'')"
}
'
```
