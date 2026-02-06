# dedup (otellogs)

The `dedup` command removes duplicate documents defined by specified fields from the search result.

## Example 1: Remove duplicates based on a single field

The following query deduplicates documents based on the `flags` field:

```ppl
source=otellogs
| dedup flags
| fields severityNumber, flags
| sort severityNumber
```

The query returns the following results:

```text
fetched rows / total rows = 2/2
+----------------+-------+
| severityNumber | flags |
|----------------+-------|
| 9              | 1     |
| 13             | 0     |
+----------------+-------+
```


## Example 2: Retain multiple duplicate documents

The following query removes duplicate documents based on the `flags` field while keeping two duplicate documents:

```ppl
source=otellogs
| dedup 2 flags
| fields severityNumber, flags
| sort severityNumber
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+-------+
| severityNumber | flags |
|----------------+-------|
| 9              | 1     |
| 13             | 0     |
| 17             | 1     |
| 21             | 0     |
+----------------+-------+
```


## Example 3: Handle documents with empty field values

The following query removes duplicate documents while keeping documents with `null` values in the specified field:

```ppl
source=otellogs
| dedup `attributes.user.email` keepempty=true
| fields severityText, `attributes.user.email`
| sort @timestamp
| head 10
```

The query returns the following results:

```text
fetched rows / total rows = 10/10
+--------------+---------------------------+
| severityText | attributes.user.email     |
|--------------+---------------------------|
| INFO         | null                      |
| ERROR        | user@example.com          |
| INFO         | admin@company.org         |
| TRACE2       | alice@wonderland.net      |
| DEBUG2       | test.user@domain.co.uk    |
| TRACE3       | support@helpdesk.io       |
| INFO3        | webhook@partner.com       |
| TRACE4       | grpc-user@service.net     |
+--------------+---------------------------+
```

The following query removes duplicate documents while ignoring documents with empty values in the specified field:

```ppl
source=otellogs
| dedup `attributes.user.email`
| fields severityText, `attributes.user.email`
| sort @timestamp
| head 7
```

The query returns the following results:

```text
fetched rows / total rows = 7/7
+--------------+---------------------------+
| severityText | attributes.user.email     |
|--------------+---------------------------|
| ERROR        | user@example.com          |
| INFO         | admin@company.org         |
| TRACE2       | alice@wonderland.net      |
| DEBUG2       | test.user@domain.co.uk    |
| TRACE3       | support@helpdesk.io       |
| INFO3        | webhook@partner.com       |
| TRACE4       | grpc-user@service.net     |
+--------------+---------------------------+
```


## Example 4: Deduplicate consecutive documents

The following query removes duplicate consecutive documents:

```ppl
source=otellogs
| sort @timestamp
| dedup flags consecutive=true
| fields severityNumber, flags
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+-------+
| severityNumber | flags |
|----------------+-------|
| 9              | 1     |
| 13             | 0     |
| 9              | 1     |
| 21             | 0     |
+----------------+-------+
```
