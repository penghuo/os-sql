/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.operator.scalar.timestamp;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.StandardTypes;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.type.DateTimes.scaleEpochMicrosToMillis;
import static java.util.Locale.ENGLISH;

@Description("Day name of the week of the given timestamp (MySQL-compatible)")
@ScalarFunction("dayname")
public class ExtractDayName
{
    private static final DateTimeFormatter DAY_NAME_FORMATTER = DateTimeFormat.forPattern("EEEE")
            .withChronology(ISOChronology.getInstanceUTC())
            .withLocale(ENGLISH);

    private ExtractDayName() {}

    @LiteralParameters("p")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice extract(@SqlType("timestamp(p)") long timestamp)
    {
        return utf8Slice(DAY_NAME_FORMATTER.print(scaleEpochMicrosToMillis(timestamp)));
    }

    @LiteralParameters("p")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice extract(@SqlType("timestamp(p)") LongTimestamp timestamp)
    {
        return extract(timestamp.getEpochMicros());
    }
}
