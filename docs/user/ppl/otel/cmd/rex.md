# rex (otellogs)

The `rex` command extracts fields from a raw text field using regular expression named capture groups.

## Example 1: Basic text extraction

The following query extracts username and domain from email addresses:

```ppl
source=otellogs
| where isnotnull(`attributes.user.email`)
| rex field=`attributes.user.email` "(?<username>[^@]+)@(?<domain>[^.]+)"
| sort @timestamp
| fields `attributes.user.email`, username, domain
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----------------------+-----------+------------+
| attributes.user.email | username  | domain     |
|-----------------------+-----------+------------|
| user@example.com      | user      | example    |
| admin@company.org     | admin     | company    |
| alice@wonderland.net  | alice     | wonderland |
| test.user@domain.co.uk| test.user | domain     |
+-----------------------+-----------+------------+
```


## Example 2: Extract complete email components

The following query extracts username, domain, and top-level domain:

```ppl
source=otellogs
| where isnotnull(`attributes.user.email`)
| rex field=`attributes.user.email` "(?<user>[a-zA-Z0-9._%+-]+)@(?<domain>[a-zA-Z0-9.-]+)\\.(?<tld>[a-zA-Z]{2,})"
| sort @timestamp
| fields `attributes.user.email`, user, domain, tld
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----------------------+-----------+------------+-----+
| attributes.user.email | user      | domain     | tld |
|-----------------------+-----------+------------+-----|
| user@example.com      | user      | example    | com |
| admin@company.org     | admin     | company    | org |
| alice@wonderland.net  | alice     | wonderland | net |
| test.user@domain.co.uk| test.user | domain.co  | uk  |
+-----------------------+-----------+------------+-----+
```


## Example 3: Replace text using sed mode

The following query uses sed mode to replace email domains:

```ppl
source=otellogs
| where isnotnull(`attributes.user.email`)
| rex field=`attributes.user.email` mode=sed "s/@.*/@company.com/"
| sort @timestamp
| fields `attributes.user.email`
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+-----------------------+
| attributes.user.email |
|-----------------------|
| user@company.com      |
| admin@company.com     |
| alice@company.com     |
| test.user@company.com |
+-----------------------+
```


## Example 4: Chain multiple rex commands

The following query extracts initial letters from severityText:

```ppl
source=otellogs
| sort @timestamp
| rex field=severityText "(?<firstchar>^.)"
| fields severityText, firstchar
| head 4
```

The query returns the following results:

```text
fetched rows / total rows = 4/4
+--------------+-----------+
| severityText | firstchar |
|--------------+-----------|
| INFO         | I         |
| ERROR        | E         |
| WARN         | W         |
| DEBUG        | D         |
+--------------+-----------+
```
