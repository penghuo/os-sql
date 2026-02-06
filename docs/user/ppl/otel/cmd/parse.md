# parse (otellogs)

The `parse` command extracts information from a text field using a regular expression and adds the extracted information to the search results.

## Example 1: Create a new field

The following query extracts the hostname from email addresses. The regex pattern `.+@(?<host>.+)` captures all characters after the `@` symbol and creates a new `host` field:

```ppl
source=otellogs
| where isnotnull(`attributes.user.email`)
| parse `attributes.user.email` '.+@(?<host>.+)'
| sort @timestamp
| fields `attributes.user.email`, host
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----------------------+----------------+
| attributes.user.email | host           |
|-----------------------+----------------|
| user@example.com      | example.com    |
| admin@company.org     | company.org    |
| alice@wonderland.net  | wonderland.net |
| test.user@domain.co.uk| domain.co.uk   |
+-----------------------+----------------+
```


## Example 2: Extract username from email

The following query extracts the username part before the `@` symbol from email addresses:

```ppl
source=otellogs
| where isnotnull(`attributes.user.email`)
| parse `attributes.user.email` '(?<username>.+)@.+'
| sort @timestamp
| fields `attributes.user.email`, username
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----------------------+-----------+
| attributes.user.email | username  |
|-----------------------+-----------|
| user@example.com      | user      |
| admin@company.org     | admin     |
| alice@wonderland.net  | alice     |
| test.user@domain.co.uk| test.user |
+-----------------------+-----------+
```


## Example 3: Extract multiple fields from email

The following query extracts both username and host from email addresses in a single parse operation:

```ppl
source=otellogs
| where isnotnull(`attributes.user.email`)
| parse `attributes.user.email` '(?<username>.+)@(?<host>.+)'
| sort @timestamp
| fields username, host
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----------+----------------+
| username  | host           |
|-----------+----------------|
| user      | example.com    |
| admin     | company.org    |
| alice     | wonderland.net |
| test.user | domain.co.uk   |
+-----------+----------------+
```
