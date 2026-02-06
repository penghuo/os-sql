# patterns (otellogs)

The `patterns` command extracts log patterns from a text field and appends the results to search results.

## Example 1: Simple pattern extraction

```ppl
source=otellogs
| patterns body method=simple_pattern
| fields body, patterns_field
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------------------------------------------------------------------------+----------------------------------------------------------------------+
| body                                                                           | patterns_field                                                       |
|--------------------------------------------------------------------------------+----------------------------------------------------------------------|
| User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to... | User <*>-<*>-<*>-<*>-<*> adding <*> of product <*> to cart           |
| Payment failed: Insufficient funds for user@example.com                        | Payment failed: Insufficient funds for <*>@<*>.<*>                   |
| Query contains Lucene special characters: +field:value -excluded AND ...       | Query contains Lucene special characters: +<*>:<*> -<*> AND ...      |
| 192.168.1.1 - - [15/Jan/2024:10:30:03 +0000] "GET /api/products?search=...     | <*>.<*>.<*>.<*> - - [<*>/<*>/<*>:<*>:<*>:<*> +<*>] "<*> /<*>/...     |
+--------------------------------------------------------------------------------+----------------------------------------------------------------------+
```


## Example 2: Pattern aggregation

```ppl
source=otellogs
| patterns body method=simple_pattern mode=aggregation
| fields patterns_field, pattern_count
| head 5
```

The query returns the following results:

```text
fetched rows / total rows = 5/5
+----------------------------------------------------------------------+---------------+
| patterns_field                                                       | pattern_count |
|----------------------------------------------------------------------+---------------|
| User <*>-<*>-<*>-<*>-<*> adding <*> of product <*> to cart           | 1             |
| Payment failed: Insufficient funds for <*>@<*>.<*>                   | 1             |
| Query contains Lucene special characters: +<*>:<*> -<*> ...          | 1             |
| <*>.<*>.<*>.<*> - - [<*>/<*>/<*>:<*>:<*>:<*> +<*>] "<*> ...          | 1             |
| Email notification sent to <*>@<*>.<*> with subject: '<*>! ...       | 1             |
+----------------------------------------------------------------------+---------------+
```


## Example 3: Brain method pattern extraction

```ppl
source=otellogs
| patterns body method=brain
| fields body, patterns_field
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+--------------------------------------------------------------------------------+----------------------------------------------------------------------+
| body                                                                           | patterns_field                                                       |
|--------------------------------------------------------------------------------+----------------------------------------------------------------------|
| User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to... | User <*> adding <*> of product <*> to cart                           |
| Payment failed: Insufficient funds for user@example.com                        | Payment failed: Insufficient funds for <*EMAIL*>                     |
| Query contains Lucene special characters: +field:value -excluded AND ...       | Query contains Lucene special characters: <*>                        |
+--------------------------------------------------------------------------------+----------------------------------------------------------------------+
```


## Example 4: Custom regex pattern

```ppl
source=otellogs
| patterns body method=simple_pattern new_field='no_numbers' pattern='[0-9]'
| fields body, no_numbers
| head 3
```

The query returns the following results:

```text
fetched rows / total rows = 3/3
+--------------------------------------------------------------------------------+----------------------------------------------------------------------+
| body                                                                           | no_numbers                                                           |
|--------------------------------------------------------------------------------+----------------------------------------------------------------------|
| User e1ce63e6-8501-11f0-930d-c2fcbdc05f14 adding 4 of product HQTGWGPNH4 to... | User e<*>ce<*><*>e<*>-<*><*><*><*>-<*><*>f<*>-<*><*><*>d-...        |
| Payment failed: Insufficient funds for user@example.com                        | Payment failed: Insufficient funds for user@example.com              |
| Query contains Lucene special characters: +field:value -excluded AND ...       | Query contains Lucene special characters: +field:value -excluded ... |
+--------------------------------------------------------------------------------+----------------------------------------------------------------------+
```

