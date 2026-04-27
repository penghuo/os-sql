/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.ppl.udf;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlNullable;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PPL rex_extract function — extracts a named capture group from a regex pattern.
 * os-sql emits REX_EXTRACT(field, pattern, group_name).
 */
public final class RexExtractFunction
{
    private RexExtractFunction() {}

    @ScalarFunction("rex_extract")
    @Description("Extracts a named capture group from a regex pattern")
    @SqlNullable
    @SqlType(StandardTypes.VARCHAR)
    public static Slice rexExtract(
            @SqlNullable @SqlType(StandardTypes.VARCHAR) Slice input,
            @SqlType(StandardTypes.VARCHAR) Slice pattern,
            @SqlType(StandardTypes.VARCHAR) Slice groupName)
    {
        if (input == null) {
            return null;
        }
        Matcher matcher = Pattern.compile(pattern.toStringUtf8()).matcher(input.toStringUtf8());
        if (matcher.find()) {
            String result = matcher.group(groupName.toStringUtf8());
            return result != null ? Slices.utf8Slice(result) : null;
        }
        return null;
    }
}
