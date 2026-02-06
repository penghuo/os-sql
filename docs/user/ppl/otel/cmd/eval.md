# eval (otellogs)

The `eval` command evaluates the specified expression and appends the result of the evaluation to the search results.

## Example 1: Create a new field

The following query creates a new `doubleSeverity` field for each document:

```ppl
source=otellogs
| sort @timestamp
| eval doubleSeverity = severityNumber * 2
| fields severityNumber, doubleSeverity
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+----------------+
| severityNumber | doubleSeverity |
|----------------+----------------|
| 9              | 18             |
| 17             | 34             |
| 13             | 26             |
| 5              | 10             |
+----------------+----------------+
```


## Example 2: Override an existing field

The following query overrides the `severityNumber` field by adding `1` to its value:

```ppl
source=otellogs
| sort @timestamp
| eval severityNumber = severityNumber + 1
| fields severityNumber
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+
| severityNumber |
|----------------|
| 10             |
| 18             |
| 14             |
| 6              |
+----------------+
```


## Example 3: Create a new field using a field defined in eval

The following query creates a new field based on another field defined in the same `eval` expression. In this example, the new `quadSeverity` field is calculated by multiplying the `doubleSeverity` field by `2`. The `doubleSeverity` field itself is defined earlier in the `eval` command:

```ppl
source=otellogs
| sort @timestamp
| eval doubleSeverity = severityNumber * 2, quadSeverity = doubleSeverity * 2
| fields severityNumber, doubleSeverity, quadSeverity
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+----------------+----------------+--------------+
| severityNumber | doubleSeverity | quadSeverity |
|----------------+----------------+--------------|
| 9              | 18             | 36           |
| 17             | 34             | 68           |
| 13             | 26             | 52           |
| 5              | 10             | 20           |
+----------------+----------------+--------------+
```


## Example 4: String concatenation

The following query uses the `+` operator for string concatenation. You can concatenate string literals and field values as follows:

```ppl
source=otellogs
| sort @timestamp
| eval greeting = 'Level: ' + severityText
| fields severityText, greeting
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+---------------+
| severityText | greeting      |
|--------------+---------------|
| INFO         | Level: INFO   |
| ERROR        | Level: ERROR  |
| WARN         | Level: WARN   |
| DEBUG        | Level: DEBUG  |
+--------------+---------------+
```


## Example 5: Multiple string concatenation with type casting

The following query performs multiple concatenation operations, including type casting from numeric values to strings:

```ppl
source=otellogs
| sort @timestamp
| eval full_info = 'Text: ' + severityText + ', Number: ' + CAST(severityNumber AS STRING)
| fields severityText, severityNumber, full_info
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+----------------+---------------------------+
| severityText | severityNumber | full_info                 |
|--------------+----------------+---------------------------|
| INFO         | 9              | Text: INFO, Number: 9     |
| ERROR        | 17             | Text: ERROR, Number: 17   |
| WARN         | 13             | Text: WARN, Number: 13    |
| DEBUG        | 5              | Text: DEBUG, Number: 5    |
+--------------+----------------+---------------------------+
```
