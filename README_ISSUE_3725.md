# Test for GitHub Issue #3725 - PPL Date Functions with Timezone

This document explains the implementation of the test case for GitHub issue #3725, which addressed a problem with PPL date functions not handling timezone parameters correctly.

## Issue Description

The issue (#3725) involved PPL date functions like `date_sub(now(), INTERVAL n DAY)` not working properly with timezone parameters. When a query was executed in a non-UTC timezone, the date comparison operations would fail to return the expected results because the timestamps were not being properly adjusted for the session timezone.

## Test Implementation

The test case (`testIssue3725` in `TimezoneParameterIT.java`) verifies the fix by:

1. Creating a test index with documents that have timestamps from the last few hours
2. Querying those documents with a date range filter using `date_sub(now(), INTERVAL 1 DAY)`
3. Verifying that the query returns the expected documents across multiple timezones

### What the Test Verifies

1. **Date filtering works correctly**: The test confirms that `date_sub(now(), INTERVAL 1 DAY)` produces the correct filter date in the specified timezone, correctly returning all documents created within the last day.

2. **Timezone parameter is respected**: The test runs with multiple timezones (UTC, America/Los_Angeles, Asia/Tokyo) to verify that each timezone is properly recognized and applied to the date functions.

3. **Date arithmetic consistency**: By comparing formatted dates for "today" and "yesterday", the test confirms that regardless of timezone, date_sub() consistently subtracts exactly one day.

## Why This Test Is Important

This test ensures that date range filtering in PPL queries works correctly with different timezone settings. This is critical for:

- Users in different geographic locations querying the same data
- Applications that need to use local time for filtering while storing data in UTC
- Consistent date arithmetic operations that respect timezone boundaries (like DST changes)

The test enhances the reliability of OpenSearch SQL's PPL implementation by ensuring that users get consistent and expected results when using date functions regardless of their configured timezone.