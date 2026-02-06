# describe (otellogs)

The `describe` command queries index metadata.

## Example 1: Fetch all metadata

```ppl
describe otellogs
```

The query returns the following results:

```text
fetched rows / total rows = 24/24
+----------------+-------------+------------+---------------------------------+-----------+-----------+...
| TABLE_CAT      | TABLE_SCHEM | TABLE_NAME | COLUMN_NAME                     | DATA_TYPE | TYPE_NAME |...
|----------------+-------------+------------+---------------------------------+-----------+-----------|...
| docTestCluster | null        | otellogs   | @timestamp                      | null      | timestamp |...
| docTestCluster | null        | otellogs   | body                            | null      | string    |...
| docTestCluster | null        | otellogs   | flags                           | null      | bigint    |...
| docTestCluster | null        | otellogs   | instrumentationScope            | null      | struct    |...
| docTestCluster | null        | otellogs   | resource                        | null      | struct    |...
| docTestCluster | null        | otellogs   | severityNumber                  | null      | bigint    |...
| docTestCluster | null        | otellogs   | severityText                    | null      | string    |...
| docTestCluster | null        | otellogs   | spanId                          | null      | string    |...
| docTestCluster | null        | otellogs   | time                            | null      | timestamp |...
| docTestCluster | null        | otellogs   | traceId                         | null      | string    |...
| docTestCluster | null        | otellogs   | attributes                      | null      | struct    |...
...
+----------------+-------------+------------+---------------------------------+-----------+-----------+...
```


## Example 2: Fetch metadata with a condition and filter

```ppl
describe otellogs
| where TYPE_NAME="bigint"
| fields COLUMN_NAME
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+----------------+
| COLUMN_NAME    |
|----------------|
| flags          |
| severityNumber |
+----------------+
```


## Example 3: Fetch string columns

```ppl
describe otellogs
| where TYPE_NAME="string"
| fields COLUMN_NAME
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+
| COLUMN_NAME  |
|--------------|
| body         |
| severityText |
| spanId       |
| traceId      |
+--------------+
```

