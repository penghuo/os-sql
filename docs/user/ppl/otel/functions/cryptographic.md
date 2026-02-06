# Cryptographic Functions (otellogs)

## MD5

```ppl
source=otellogs
| eval `MD5('hello')` = MD5('hello')
| fields `MD5('hello')`
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+----------------------------------+
| MD5('hello')                     |
|----------------------------------|
| 5d41402abc4b2a76b9719d911017c592 |
+----------------------------------+
```


## SHA1

```ppl
source=otellogs
| eval `SHA1('hello')` = SHA1('hello')
| fields `SHA1('hello')`
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+------------------------------------------+
| SHA1('hello')                            |
|------------------------------------------|
| aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d |
+------------------------------------------+
```


## SHA2

```ppl
source=otellogs
| eval `SHA2('hello',256)` = SHA2('hello',256)
| fields `SHA2('hello',256)`
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+------------------------------------------------------------------+
| SHA2('hello',256)                                                |
|------------------------------------------------------------------|
| 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824 |
+------------------------------------------------------------------+
```

Example with SHA-512:

```ppl
source=otellogs
| eval `SHA2('hello',512)` = SHA2('hello',512)
| fields `SHA2('hello',512)`
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+----------------------------------------------------------------------------------------------------------------------------------+
| SHA2('hello',512)                                                                                                                |
|----------------------------------------------------------------------------------------------------------------------------------|
| 9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043 |
+----------------------------------------------------------------------------------------------------------------------------------+
```

