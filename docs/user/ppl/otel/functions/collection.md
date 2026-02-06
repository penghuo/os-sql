# Collection Functions (otellogs)

## ARRAY

```ppl
source=otellogs
| eval arr = array(1, 2, 3)
| fields arr
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------+
| arr     |
|---------|
| [1,2,3] |
+---------+
```

Example with mixed types:

```ppl
source=otellogs
| eval arr = array(1, "demo")
| fields arr
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+----------+
| arr      |
|----------|
| [1,demo] |
+----------+
```


## ARRAY_LENGTH

```ppl
source=otellogs
| eval arr = array(1, 2, 3)
| eval length = array_length(arr)
| fields length
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------+
| length |
|--------|
| 3      |
+--------+
```


## FORALL

```ppl
source=otellogs
| eval arr = array(1, 2, 3), result = forall(arr, x -> x > 0)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| True   |
+--------+
```


## EXISTS

```ppl
source=otellogs
| eval arr = array(-1, -2, 3), result = exists(arr, x -> x > 0)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| True   |
+--------+
```


## FILTER

```ppl
source=otellogs
| eval arr = array(1, -2, 3), result = filter(arr, x -> x > 0)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| [1,3]  |
+--------+
```


## TRANSFORM

```ppl
source=otellogs
| eval arr = array(1, -2, 3), result = transform(arr, x -> x + 2)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [3,0,5] |
+---------+
```

Example with index:

```ppl
source=otellogs
| eval arr = array(1, -2, 3), result = transform(arr, (x, i) -> x + i)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+----------+
| result   |
|----------|
| [1,-1,5] |
+----------+
```


## REDUCE

```ppl
source=otellogs
| eval arr = array(1, -2, 3), result = reduce(arr, 10, (acc, x) -> acc + x)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| 12     |
+--------+
```

Example with reduce function:

```ppl
source=otellogs
| eval arr = array(1, -2, 3), result = reduce(arr, 10, (acc, x) -> acc + x, acc -> acc * 10)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| 120    |
+--------+
```


## MVJOIN

```ppl
source=otellogs
| eval result = mvjoin(array('a', 'b', 'c'), ',')
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| a,b,c  |
+--------+
```


## MVAPPEND

```ppl
source=otellogs
| eval result = mvappend(1, 1, 3)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [1,1,3] |
+---------+
```

Example with arrays:

```ppl
source=otellogs
| eval result = mvappend(1, array(2, 3))
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [1,2,3] |
+---------+
```


## SPLIT

```ppl
source=otellogs
| eval test = 'buttercup;rarity;tenderhoof;dash', result = split(test, ';')
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+------------------------------------+
| result                             |
|------------------------------------|
| [buttercup,rarity,tenderhoof,dash] |
+------------------------------------+
```


## MVDEDUP

```ppl
source=otellogs
| eval arr = array(1, 2, 2, 3, 1, 4), result = mvdedup(arr)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+-----------+
| result    |
|-----------|
| [1,2,3,4] |
+-----------+
```


## MVFIND

```ppl
source=otellogs
| eval arr = array('apple', 'banana', 'apricot'), result = mvfind(arr, 'ban.*')
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| 1      |
+--------+
```


## MVINDEX

```ppl
source=otellogs
| eval arr = array('a', 'b', 'c', 'd', 'e'), result = mvindex(arr, 1)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+--------+
| result |
|--------|
| b      |
+--------+
```

Example with range:

```ppl
source=otellogs
| eval arr = array(1, 2, 3, 4, 5), result = mvindex(arr, 1, 3)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+---------+
| result  |
|---------|
| [2,3,4] |
+---------+
```


## MVMAP

```ppl
source=otellogs
| eval arr = array(1, 2, 3), result = mvmap(arr, arr * 10)
| fields result
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+------------+
| result     |
|------------|
| [10,20,30] |
+------------+
```


## MVZIP

```ppl
source=otellogs
| eval hosts = array('host1', 'host2'), ports = array('80', '443'), nserver = mvzip(hosts, ports, ':')
| fields nserver
| head 1
```

Expected output:

```text
fetched rows / total rows = 1/1
+----------------------+
| nserver              |
|----------------------|
| [host1:80,host2:443] |
+----------------------+
```

