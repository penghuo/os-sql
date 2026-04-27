/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.ppl.udf;

import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

/**
 * PPL DIVIDE function — os-sql emits DIVIDE(x, y) instead of x / y.
 */
public final class DivideFunction
{
    private DivideFunction() {}

    @ScalarFunction("divide")
    @Description("Divides two doubles")
    @SqlType(StandardTypes.DOUBLE)
    public static double divideDouble(@SqlType(StandardTypes.DOUBLE) double a, @SqlType(StandardTypes.DOUBLE) double b)
    {
        return a / b;
    }

    @ScalarFunction("divide")
    @Description("Divides two bigints")
    @SqlType(StandardTypes.DOUBLE)
    public static double divideLong(@SqlType(StandardTypes.BIGINT) long a, @SqlType(StandardTypes.BIGINT) long b)
    {
        return (double) a / b;
    }

    @ScalarFunction("divide")
    @Description("Divides double by bigint")
    @SqlType(StandardTypes.DOUBLE)
    public static double divideMixed(@SqlType(StandardTypes.DOUBLE) double a, @SqlType(StandardTypes.BIGINT) long b)
    {
        return a / b;
    }
}
