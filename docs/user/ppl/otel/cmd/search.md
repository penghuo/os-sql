# search (otellogs)

The `search` command retrieves documents from the index.

## Example 1: Fetching all data

Retrieve all documents from an index by specifying only the source:

```ppl
source=otellogs
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


## Example 2: Text search

For basic text search, use an unquoted single term:

```ppl
search ERROR source=otellogs
| sort @timestamp
| fields severityText, body
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+--------------+---------------------------------------------------------+
| severityText | body                                                    |
|--------------+---------------------------------------------------------|
| ERROR        | Payment failed: Insufficient funds for user@example.com |
+--------------+---------------------------------------------------------+
```

Phrase search requires quotation marks for multi-word exact matching:

```ppl
search "Payment failed" source=otellogs
| fields body
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------------------------------------------------------+
| body                                                    |
|---------------------------------------------------------|
| Payment failed: Insufficient funds for user@example.com |
+---------------------------------------------------------+
```


## Example 3: Boolean logic and operator precedence

Use `OR` to match documents containing any of the specified conditions:

```ppl
search severityText="ERROR" OR severityText="FATAL" source=otellogs
| sort @timestamp
| fields severityText
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+--------------+
| severityText |
|--------------|
| ERROR        |
| FATAL        |
| ERROR        |
+--------------+
```

Combine conditions with `AND`:

```ppl
search severityText="INFO" AND `resource.attributes.service.name`="cart-service" source=otellogs
| fields body
| head 1
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+----------------------------------------------------------------------------------+
| body                                                                             |
|----------------------------------------------------------------------------------|
| User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to cart |
+----------------------------------------------------------------------------------+
```


## Example 4: Range queries

Use comparison operators to filter numeric fields:

```ppl
search severityNumber>15 AND severityNumber<=20 source=otellogs
| sort @timestamp
| fields severityNumber
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+----------------+
| severityNumber |
|----------------|
| 17             |
| 17             |
| 18             |
+----------------+
```


## Example 5: Wildcards

Use `*` to match any number of characters:

```ppl
search severityText=ERR* source=otellogs
| sort @timestamp
| fields severityText
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+--------------+
| severityText |
|--------------|
| ERROR        |
| ERROR        |
| ERROR2       |
+--------------+
```

Use `?` to match exactly one character:

```ppl
search severityText="INFO?" source=otellogs
| sort @timestamp
| fields severityText
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+--------------+
| severityText |
|--------------|
| INFO2        |
| INFO3        |
| INFO4        |
+--------------+
```


## Example 6: Field value matching with IN

Check whether a field matches any value from a predefined list:

```ppl
search severityText IN ("ERROR", "WARN", "FATAL") source=otellogs
| sort @timestamp
| fields severityText
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+--------------+
| severityText |
|--------------|
| ERROR        |
| WARN         |
| FATAL        |
+--------------+
```


## Example 7: Search by nested attribute

Search for logs containing a specific user email address in the attributes:

```ppl
search `attributes.user.email`="user@example.com" source=otellogs
| fields body
```

The query returns the following results:

```text
fetched rows / total rows = 1/1
+---------------------------------------------------------+
| body                                                    |
|---------------------------------------------------------|
| Payment failed: Insufficient funds for user@example.com |
+---------------------------------------------------------+
```


## Example 8: Complex expressions

Combine multiple conditions using Boolean operators and parentheses:

```ppl
search (severityText="ERROR" OR severityText="WARN") AND severityNumber>10 source=otellogs
| sort @timestamp
| fields severityText
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+--------------+
| severityText |
|--------------|
| ERROR        |
| WARN         |
| ERROR        |
+--------------+
```
