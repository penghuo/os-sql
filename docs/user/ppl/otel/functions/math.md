# Mathematical Functions (otellogs)

## ABS

```ppl
source=otellogs
| eval `ABS(-1)` = ABS(-1)
| fields `ABS(-1)`
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------+
| ABS(-1) |
|---------|
| 1       |
+---------+
```

## ADD

```ppl
source=otellogs
| sort @timestamp
| eval `ADD(severityNumber, 10)` = ADD(severityNumber, 10)
| fields severityNumber, `ADD(severityNumber, 10)`
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+----------------+-------------------------+
| severityNumber | ADD(severityNumber, 10) |
|----------------+-------------------------|
| 9              | 19                      |
| 17             | 27                      |
| 13             | 23                      |
| 5              | 15                      |
+----------------+-------------------------+
```

## SUBTRACT

```ppl
source=otellogs
| sort @timestamp
| eval `SUBTRACT(severityNumber, 5)` = SUBTRACT(severityNumber, 5)
| fields severityNumber, `SUBTRACT(severityNumber, 5)`
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+----------------+-----------------------------+
| severityNumber | SUBTRACT(severityNumber, 5) |
|----------------+-----------------------------|
| 9              | 4                           |
| 17             | 12                          |
| 13             | 8                           |
| 5              | 0                           |
+----------------+-----------------------------+
```

## MULTIPLY

```ppl
source=otellogs
| sort @timestamp
| eval `MULTIPLY(severityNumber, 2)` = MULTIPLY(severityNumber, 2)
| fields severityNumber, `MULTIPLY(severityNumber, 2)`
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+----------------+-----------------------------+
| severityNumber | MULTIPLY(severityNumber, 2) |
|----------------+-----------------------------|
| 9              | 18                          |
| 17             | 34                          |
| 13             | 26                          |
| 5              | 10                          |
+----------------+-----------------------------+
```

## DIVIDE

```ppl
source=otellogs
| sort @timestamp
| eval `DIVIDE(severityNumber, 2)` = DIVIDE(severityNumber, 2)
| fields severityNumber, `DIVIDE(severityNumber, 2)`
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+----------------+---------------------------+
| severityNumber | DIVIDE(severityNumber, 2) |
|----------------+---------------------------|
| 9              | 4                         |
| 17             | 8                         |
| 13             | 6                         |
| 5              | 2                         |
+----------------+---------------------------+
```

## CEIL

```ppl
source=otellogs
| eval `CEIL(12.34)` = CEIL(12.34)
| fields `CEIL(12.34)`
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-------------+
| CEIL(12.34) |
|-------------|
| 13.0        |
+-------------+
```

## FLOOR

```ppl
source=otellogs
| eval `FLOOR(12.34)` = FLOOR(12.34)
| fields `FLOOR(12.34)`
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------------+
| FLOOR(12.34) |
|--------------|
| 12.0         |
+--------------+
```

## ROUND

```ppl
source=otellogs
| eval `ROUND(12.567, 2)` = ROUND(12.567, 2)
| fields `ROUND(12.567, 2)`
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+------------------+
| ROUND(12.567, 2) |
|------------------|
| 12.57            |
+------------------+
```

## MOD

```ppl
source=otellogs
| sort @timestamp
| eval `MOD(severityNumber, 5)` = MOD(severityNumber, 5)
| fields severityNumber, `MOD(severityNumber, 5)`
| head 4
```

Expected output:

```text
fetched rows / total rows = 4/4
+----------------+------------------------+
| severityNumber | MOD(severityNumber, 5) |
|----------------+------------------------|
| 9              | 4                      |
| 17             | 2                      |
| 13             | 3                      |
| 5              | 0                      |
+----------------+------------------------+
```

## SQRT

```ppl
source=otellogs
| eval `SQRT(16)` = SQRT(16)
| fields `SQRT(16)`
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+----------+
| SQRT(16) |
|----------|
| 4.0      |
+----------+
```

## POW

```ppl
source=otellogs
| eval `POW(2, 3)` = POW(2, 3)
| fields `POW(2, 3)`
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-----------+
| POW(2, 3) |
|-----------|
| 8.0       |
+-----------+
```

## LOG

```ppl
source=otellogs
| eval `LOG(10)` = LOG(10)
| fields `LOG(10)`
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------------------+
| LOG(10)            |
|--------------------|
| 2.302585092994046  |
+--------------------+
```

## LOG10

```ppl
source=otellogs
| eval `LOG10(100)` = LOG10(100)
| fields `LOG10(100)`
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+------------+
| LOG10(100) |
|------------|
| 2.0        |
+------------+
```

## PI

```ppl
source=otellogs
| eval `PI()` = PI()
| fields `PI()`
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-------------------+
| PI()              |
|-------------------|
| 3.141592653589793 |
+-------------------+
```

## E

```ppl
source=otellogs
| eval `E()` = E()
| fields `E()`
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-------------------+
| E()               |
|-------------------|
| 2.718281828459045 |
+-------------------+
```
