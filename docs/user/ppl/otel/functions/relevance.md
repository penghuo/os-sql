# Relevance Functions (otellogs)

The relevance functions enable searching the index for documents by relevance of the input query.

## MATCH

```ppl
source=otellogs
| where match(body, 'error')
| fields severityText, body
| head 3
```

Expected output:

```text
fetched rows / total rows = 3/3
+--------------+--------------------------------------------------------------------------------+
| severityText | body                                                                           |
|--------------+--------------------------------------------------------------------------------|
| ERROR        | Payment failed: Insufficient funds for user@example.com                        |
| ERROR        | Failed to parse JSON with special characters: {"key": "value with \"quotes\"...  |
| ERROR2       | Elasticsearch query failed: {"query":{"bool":{"must":[{"match":{"email":"*@... |
+--------------+--------------------------------------------------------------------------------+
```

Example with options:

```ppl
source=otellogs
| where match(body, 'payment', operator='AND', boost=2.0)
| fields severityText, body
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------------+----------------------------------------------------------+
| severityText | body                                                     |
|--------------+----------------------------------------------------------|
| ERROR        | Payment failed: Insufficient funds for user@example.com  |
+--------------+----------------------------------------------------------+
```


## MATCH_PHRASE

```ppl
source=otellogs
| where match_phrase(body, 'failed to')
| fields severityText, body
| head 3
```

Expected output:

```text
fetched rows / total rows = 3/3
+--------------+--------------------------------------------------------------------------------+
| severityText | body                                                                           |
|--------------+--------------------------------------------------------------------------------|
| ERROR        | Failed to parse JSON with special characters: {"key": "value with \"quotes\"...  |
| ERROR3       | Failed to send email to multiple recipients: invalid@, not-an-email, ...       |
| ERROR4       | Failed to process message from queue: Invalid JSON in message body ...         |
+--------------+--------------------------------------------------------------------------------+
```


## MULTI_MATCH

```ppl
source=otellogs
| where multi_match(['body'], 'user service')
| fields severityText, body
| head 3
```

Expected output:

```text
fetched rows / total rows = 3/3
+--------------+--------------------------------------------------------------------------------+
| severityText | body                                                                           |
|--------------+--------------------------------------------------------------------------------|
| INFO         | User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product ...              |
| TRACE        | Executing SQL: SELECT * FROM users WHERE email LIKE '%@gmail.com' ...          |
| FATAL2       | System shutdown initiated: Out of memory error at UserService.java:142        |
+--------------+--------------------------------------------------------------------------------+
```


## QUERY_STRING

```ppl
source=otellogs
| where query_string(['body'], 'Database OR connection')
| fields severityText, body
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------------+-----------------------------------------------------------------------+
| severityText | body                                                                  |
|--------------+-----------------------------------------------------------------------|
| FATAL        | Database connection pool exhausted: postgresql://db.example.com:5432  |
+--------------+-----------------------------------------------------------------------+
```


## SIMPLE_QUERY_STRING

```ppl
source=otellogs
| where simple_query_string(['body'], 'memory error')
| fields severityText, body
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------------+---------------------------------------------------------------------+
| severityText | body                                                                |
|--------------+---------------------------------------------------------------------|
| FATAL2       | System shutdown initiated: Out of memory error at UserService.java:142 |
+--------------+---------------------------------------------------------------------+
```

