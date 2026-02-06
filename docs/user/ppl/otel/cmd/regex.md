# regex (otellogs)

The `regex` command filters search results by matching field values against a regular expression pattern.

## Example 1: Basic pattern matching

The following query returns documents where the severityText starts with `ERR`:

```ppl
source=otellogs
| regex severityText="^ERR.*"
| sort @timestamp
| fields severityNumber, severityText
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 17             | ERROR        |
| 17             | ERROR        |
| 18             | ERROR2       |
+----------------+--------------+
```


## Example 2: Negative matching

The following query excludes documents where the severityText ends with a digit:

```ppl
source=otellogs
| regex severityText!=".*[0-9]$"
| sort @timestamp
| fields severityNumber, severityText
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+--------------+
| severityNumber | severityText |
|----------------+--------------|
| 9              | INFO         |
| 17             | ERROR        |
| 13             | WARN         |
| 5              | DEBUG        |
+----------------+--------------+
```


## Example 3: Email domain matching

The following query filters logs by email domain pattern:

```ppl
source=otellogs
| where isnotnull(`attributes.user.email`)
| regex `attributes.user.email`="@example\.com$"
| fields `attributes.user.email`, severityText
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+-----------------------+--------------+
| attributes.user.email | severityText |
|-----------------------+--------------|
| user@example.com      | ERROR        |
+-----------------------+--------------+
```


## Example 4: Case-insensitive matching with inline flag

The following query uses case-insensitive matching:

```ppl
source=otellogs
| regex severityText="(?i)info.*"
| sort @timestamp
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
| INFO         |
| INFO         |
| INFO2        |
+--------------+
```
