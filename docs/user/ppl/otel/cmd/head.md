# head (otellogs)

The `head` command returns the first N lines from a search result.

## Example 1: Retrieve the first set of results using the default size

The following query returns the default number of search results (10):

```ppl
source=otellogs
| sort @timestamp
| fields severityText, severityNumber
| head
```

The query returns the following results:

```text
fetched rows / total rows = 10/10
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| INFO         | 9              |
| ERROR        | 17             |
| WARN         | 13             |
| DEBUG        | 5              |
| INFO         | 9              |
| FATAL        | 21             |
| TRACE        | 1              |
| ERROR        | 17             |
| WARN         | 13             |
| INFO         | 9              |
+--------------+----------------+
```


## Example 2: Retrieve a specified number of results

The following query returns the first 3 search results:

```ppl
source=otellogs
| sort @timestamp
| fields severityText, severityNumber
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| INFO         | 9              |
| ERROR        | 17             |
| WARN         | 13             |
+--------------+----------------+
```


## Example 3: Retrieve the first N results after an offset M

The following query demonstrates how to retrieve the first 3 results starting with the second result from the `otellogs` index:

```ppl
source=otellogs
| sort @timestamp
| fields severityText, severityNumber
| head 3 from 1
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| ERROR        | 17             |
| WARN         | 13             |
| DEBUG        | 5              |
+--------------+----------------+
```
