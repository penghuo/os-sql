===========
Expressions
===========

.. rubric:: Table of contents

.. contents::
   :local:
   :depth: 3


Introduction
============

Expressions, particularly value expressions, are those which return a scalar value. Expressions have different types and forms. For example, there are literal values as atom expression and arithmetic, predicate and function expression built on top of them. And also expressions can be used in different clauses, such as using arithmetic expression in ``Filter``, ``Stats`` command.

Logical Operators
=================


Arithmetic Operators
====================

Description
-----------

Operators
`````````

Arithmetic expression is an expression formed by numeric literals and binary arithmetic operators as follows:

1. ``+``: Add.
2. ``-``: Subtract.
3. ``*``: Multiply.
4. ``/``: Divide. For integers, the result is an integer with fractional part discarded.
5. ``%``: Modulo. This can be used with integers only with remainder of the division as result.

Precedence
``````````

Parentheses can be used to control the precedence of arithmetic operators. Otherwise, operators of higher precedence is performed first.

Type Conversion
```````````````

Implicit type conversion is performed when looking up operator signature. For example, an integer ``+`` a real number matches signature ``+(double,double)`` which results in a real number. This rule also applies to function call discussed below.

Examples
--------

Here is an example for different type of arithmetic expressions::

    os> source account | where age > (25 + 5) | fields age ;
    fetched rows / total rows = 1/1
    +---------+---------------+-------------+
    | 1 + 2   | (9 - 1) % 3   | 2 * 4 / 3   |
    |---------+---------------+-------------|
    | 3       | 2             | 2           |
    +---------+---------------+-------------+

Predicate Operators
===================

Description
-----------

Predicate operator is an expression that evaluated to be ture. The MISSING and NULL value comparison has following the rule. MISSING value only equal to MISSING value and less than all the other values. NULL value equals to NULL value, large than MISSING value, but less than all the other values.

Operators
`````````

+----------------+----------------------------------------+
| name           | description                            |
+----------------+----------------------------------------+
| >              | Greater than operator                  |
+----------------+----------------------------------------+
| >=             | Greater than or equal operator         |
+----------------+----------------------------------------+
| <              | Less than operator                     |
+----------------+----------------------------------------+
| !=             | Not equal operator                     |
+----------------+----------------------------------------+
| <=             | Less than or equal operator            |
+----------------+----------------------------------------+
| =              | Equal operator                         |
+----------------+----------------------------------------+
| LIKE           | Simple Pattern matching                |
+----------------+----------------------------------------+
| IN             | NULL value test                        |
+----------------+----------------------------------------+
| AND            | AND operator                           |
+----------------+----------------------------------------+
| OR             | OR operator                            |
+----------------+----------------------------------------+
| XOR            | XOR operator                           |
+----------------+----------------------------------------+
| NOT            | NOT NULL value test                    |
+----------------+----------------------------------------+

Basic Predicate Operator
------------------------

Here is an example for comparison operators::

    os> source account | where age > 33 | fields age ;
    fetched rows / total rows = 1/1
    +---------+----------+---------+----------+----------+---------+
    | 2 > 1   | 2 >= 1   | 2 < 1   | 2 != 1   | 2 <= 1   | 2 = 1   |
    |---------+----------+---------+----------+----------+---------|
    | True    | True     | False   | True     | False    | False   |
    +---------+----------+---------+----------+----------+---------+


IN
--

IN operator test field in value lists::

    os> source account | where age in (32, 33) | fields age ;
    fetched rows / total rows = 1/1
    +----------------------+--------------------+--------------------------+------------------------+
    | 'axyzb' LIKE 'a%b'   | 'acb' LIKE 'a_b'   | 'axyzb' NOT LIKE 'a%b'   | 'acb' NOT LIKE 'a_b'   |
    |----------------------+--------------------+--------------------------+------------------------|
    | True                 | True               | False                    | False                  |
    +----------------------+--------------------+--------------------------+------------------------+


AND
---

AND operator ::

    os> source account | where age = 32 OR age = 33 | fields age ;
    fetched rows / total rows = 1/1
    +----------------------+--------------------+--------------------------+------------------------+
    | 'axyzb' LIKE 'a%b'   | 'acb' LIKE 'a_b'   | 'axyzb' NOT LIKE 'a%b'   | 'acb' NOT LIKE 'a_b'   |
    |----------------------+--------------------+--------------------------+------------------------|
    | True                 | True               | False                    | False                  |
    +----------------------+--------------------+--------------------------+------------------------+


NOT
---

NOT operator ::

    os> source account | where not age in (32, 33) | fields age ;
    fetched rows / total rows = 1/1
    +----------------------+--------------------+--------------------------+------------------------+
    | 'axyzb' LIKE 'a%b'   | 'acb' LIKE 'a_b'   | 'axyzb' NOT LIKE 'a%b'   | 'acb' NOT LIKE 'a_b'   |
    |----------------------+--------------------+--------------------------+------------------------|
    | True                 | True               | False                    | False                  |
    +----------------------+--------------------+--------------------------+------------------------+

