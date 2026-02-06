# replace (otellogs)

The `replace` command replaces text in one or more fields in the search results.

## Example 1: Replace text in one field

```ppl
source=otellogs
| sort @timestamp
| replace "INFO" WITH "INFORMATION" IN severityText
| fields severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-------------+
| severityText |
|-------------|
| INFORMATION |
| ERROR       |
| WARN        |
| DEBUG       |
+-------------+
```


## Example 2: Replace text using multiple pattern-replacement pairs

```ppl
source=otellogs
| sort @timestamp
| replace "INFO" WITH "INFORMATION", "ERROR" WITH "ERR" IN severityText
| fields severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-------------+
| severityText |
|-------------|
| INFORMATION |
| ERR         |
| WARN        |
| DEBUG       |
+-------------+
```


## Example 3: Wildcard suffix matching

```ppl
source=otellogs
| sort @timestamp
| replace "*OR" WITH "REPLACED" IN severityText
| fields severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+
| severityText |
|--------------|
| INFO         |
| REPLACED     |
| WARN         |
| DEBUG        |
+--------------+
```


## Example 4: Replace in pipeline with filter

```ppl
source=otellogs
| where severityNumber > 15
| sort @timestamp
| replace "ERROR" WITH "ERR" IN severityText
| fields severityText, severityNumber
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+--------------+----------------+
| severityText | severityNumber |
|--------------+----------------|
| ERR          | 17             |
| FATAL        | 21             |
| ERR          | 17             |
+--------------+----------------+
```
