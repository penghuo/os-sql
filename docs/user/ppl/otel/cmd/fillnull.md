# fillnull (otellogs)

The `fillnull` command replaces `null` values in one or more fields of the search results with a specified value.

## Example 1: Replace null values in a single field with a specified value

The following query replaces null values in the `attributes.user.email` field with `\<not found\>`:

```ppl
source=otellogs
| sort @timestamp
| fields `attributes.user.email`, severityText
| fillnull with '<not found>' in `attributes.user.email`
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+-----------------------+--------------+
| attributes.user.email | severityText |
|-----------------------+--------------|
| <not found>           | INFO         |
| user@example.com      | ERROR        |
| <not found>           | WARN         |
| <not found>           | DEBUG        |
| <not found>           | INFO         |
+-----------------------+--------------+
```


## Example 2: Replace null values in multiple fields with a specified value

The following query replaces null values in both the `attributes.user.email` and `traceId` fields with `\<not found\>`:

```ppl
source=otellogs
| sort @timestamp
| fields `attributes.user.email`, traceId
| fillnull with '<not found>' in `attributes.user.email`, traceId
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+-----------------------+----------------------------------+
| attributes.user.email | traceId                          |
|-----------------------+----------------------------------|
| <not found>           | b3cb01a03c846973fd496b973f49be85 |
| user@example.com      | 7475a30207dbef54d29e42c37f09a528 |
| <not found>           | <not found>                      |
| <not found>           | <not found>                      |
| <not found>           | <not found>                      |
+-----------------------+----------------------------------+
```


## Example 3: Replace null values in all fields with a specified value

The following query replaces null values in all fields when no `field-list` is specified:

```ppl
source=otellogs
| sort @timestamp
| fields `attributes.user.email`, traceId
| fillnull with '<not found>'
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+-----------------------+----------------------------------+
| attributes.user.email | traceId                          |
|-----------------------+----------------------------------|
| <not found>           | b3cb01a03c846973fd496b973f49be85 |
| user@example.com      | 7475a30207dbef54d29e42c37f09a528 |
| <not found>           | <not found>                      |
| <not found>           | <not found>                      |
| <not found>           | <not found>                      |
+-----------------------+----------------------------------+
```


## Example 4: Replace null values in multiple fields with different specified values

The following query shows how to use the `fillnull` command with different replacement values for multiple fields using the `using` syntax:

```ppl
source=otellogs
| sort @timestamp
| fields `attributes.user.email`, traceId
| fillnull using `attributes.user.email` = '<no email>', traceId = '<no trace>'
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+-----------------------+----------------------------------+
| attributes.user.email | traceId                          |
|-----------------------+----------------------------------|
| <no email>            | b3cb01a03c846973fd496b973f49be85 |
| user@example.com      | 7475a30207dbef54d29e42c37f09a528 |
| <no email>            | <no trace>                       |
| <no email>            | <no trace>                       |
| <no email>            | <no trace>                       |
+-----------------------+----------------------------------+
```


## Example 5: Replace null values in specific fields using the value= syntax

The following query shows how to use the `fillnull` command with the `value=` syntax to replace null values in specific fields:

```ppl
source=otellogs
| sort @timestamp
| fields `attributes.user.email`, traceId
| fillnull value="<not found>" `attributes.user.email` traceId
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+-----------------------+----------------------------------+
| attributes.user.email | traceId                          |
|-----------------------+----------------------------------|
| <not found>           | b3cb01a03c846973fd496b973f49be85 |
| user@example.com      | 7475a30207dbef54d29e42c37f09a528 |
| <not found>           | <not found>                      |
| <not found>           | <not found>                      |
| <not found>           | <not found>                      |
+-----------------------+----------------------------------+
```


## Example 6: Replace null values in all fields using the value= syntax

When no `field-list` is specified, the replacement applies to all fields in the result:

```ppl
source=otellogs
| sort @timestamp
| fields `attributes.user.email`, traceId
| fillnull value='<not found>'
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+-----------------------+----------------------------------+
| attributes.user.email | traceId                          |
|-----------------------+----------------------------------|
| <not found>           | b3cb01a03c846973fd496b973f49be85 |
| user@example.com      | 7475a30207dbef54d29e42c37f09a528 |
| <not found>           | <not found>                      |
| <not found>           | <not found>                      |
| <not found>           | <not found>                      |
+-----------------------+----------------------------------+
```
