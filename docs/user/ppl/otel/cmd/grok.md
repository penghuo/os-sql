# grok (otellogs)

The `grok` command parses a text field using a Grok pattern and appends the extracted results to the search results.

## Example 1: Create a new field

The following query shows how to use the `grok` command to create a new field, `host`, for each document. The `host` field captures the hostname following `@` in the email field:

```ppl
source=otellogs
| where isnotnull(`attributes.user.email`)
| grok `attributes.user.email` '.+@%{HOSTNAME:host}'
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


## Example 2: Extract data using GREEDYDATA pattern

The following query uses the `GREEDYDATA` pattern to extract the username portion of the email address:

```ppl
source=otellogs
| where isnotnull(`attributes.user.email`)
| grok `attributes.user.email` '%{GREEDYDATA:username}@%{HOSTNAME}'
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
