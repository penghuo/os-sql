/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.ppl.udf;

import io.airlift.slice.Slice;
import io.trino.spi.block.SqlMap;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

/**
 * PPL query_string function — pushed down to Lucene QueryStringQueryParser.
 * This UDF exists so Trino's planner can resolve the function during planning.
 * The actual execution is pushed down to the OpenSearch connector via applyFilter();
 * this body is only called if pushdown fails (returns true = no filtering).
 */
public final class QueryStringFunction
{
    private QueryStringFunction() {}

    @ScalarFunction("query_string")
    @Description("OpenSearch query_string filter — pushed down to Lucene")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean queryString(@SqlType(StandardTypes.VARCHAR) Slice query)
    {
        return true;
    }

    @ScalarFunction("query_string")
    @Description("OpenSearch query_string filter with named args — pushed down to Lucene")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean queryStringMap(@SqlType("map(varchar,varchar)") SqlMap args)
    {
        // Fallback: if not pushed down, don't filter
        return true;
    }
}
