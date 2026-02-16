/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;

class PartialAggStateTest {

    @Nested
    @DisplayName("CountPartialAggState")
    class CountTests {

        @Test
        @DisplayName("accumulate and produce final count")
        void accumulateAndFinalResult() {
            CountPartialAggState state = new CountPartialAggState();
            state.accumulate(ExprValueUtils.integerValue(10));
            state.accumulate(ExprValueUtils.integerValue(20));
            state.accumulate(ExprValueUtils.integerValue(30));

            assertEquals(ExprValueUtils.longValue(3L), state.finalResult());
        }

        @Test
        @DisplayName("empty count returns zero")
        void emptyCountReturnsZero() {
            CountPartialAggState state = new CountPartialAggState();
            assertEquals(ExprValueUtils.longValue(0L), state.finalResult());
        }

        @Test
        @DisplayName("serialize and merge partial counts")
        void serializeAndMerge() {
            // Partition 1: count = 3
            CountPartialAggState partial1 = new CountPartialAggState();
            partial1.accumulate(ExprValueUtils.integerValue(1));
            partial1.accumulate(ExprValueUtils.integerValue(2));
            partial1.accumulate(ExprValueUtils.integerValue(3));

            Map<String, ExprValue> row1 = new LinkedHashMap<>();
            partial1.toPartialResult("cnt", row1);
            assertEquals(ExprValueUtils.longValue(3L), row1.get("cnt_count"));

            // Partition 2: count = 2
            CountPartialAggState partial2 = new CountPartialAggState();
            partial2.accumulate(ExprValueUtils.integerValue(4));
            partial2.accumulate(ExprValueUtils.integerValue(5));

            Map<String, ExprValue> row2 = new LinkedHashMap<>();
            partial2.toPartialResult("cnt", row2);

            // Final merge
            CountPartialAggState finalState = new CountPartialAggState();
            finalState.mergePartialResult("cnt", row1);
            finalState.mergePartialResult("cnt", row2);

            assertEquals(ExprValueUtils.longValue(5L), finalState.finalResult());
        }
    }

    @Nested
    @DisplayName("SumPartialAggState")
    class SumTests {

        @Test
        @DisplayName("accumulate and produce final sum for integers")
        void accumulateIntegerSum() {
            SumPartialAggState state = new SumPartialAggState(ExprCoreType.INTEGER);
            state.accumulate(ExprValueUtils.integerValue(10));
            state.accumulate(ExprValueUtils.integerValue(20));
            state.accumulate(ExprValueUtils.integerValue(30));

            assertEquals(ExprValueUtils.integerValue(60), state.finalResult());
        }

        @Test
        @DisplayName("accumulate and produce final sum for doubles")
        void accumulateDoubleSum() {
            SumPartialAggState state = new SumPartialAggState(ExprCoreType.DOUBLE);
            state.accumulate(ExprValueUtils.doubleValue(1.5));
            state.accumulate(ExprValueUtils.doubleValue(2.5));

            assertEquals(ExprValueUtils.doubleValue(4.0), state.finalResult());
        }

        @Test
        @DisplayName("empty sum returns null")
        void emptySumReturnsNull() {
            SumPartialAggState state = new SumPartialAggState(ExprCoreType.INTEGER);
            assertTrue(state.finalResult().isNull());
        }

        @Test
        @DisplayName("serialize and merge partial sums")
        void serializeAndMerge() {
            SumPartialAggState partial1 = new SumPartialAggState(ExprCoreType.LONG);
            partial1.accumulate(ExprValueUtils.longValue(10L));
            partial1.accumulate(ExprValueUtils.longValue(20L));

            Map<String, ExprValue> row1 = new LinkedHashMap<>();
            partial1.toPartialResult("total", row1);

            SumPartialAggState partial2 = new SumPartialAggState(ExprCoreType.LONG);
            partial2.accumulate(ExprValueUtils.longValue(30L));

            Map<String, ExprValue> row2 = new LinkedHashMap<>();
            partial2.toPartialResult("total", row2);

            SumPartialAggState finalState = new SumPartialAggState(ExprCoreType.LONG);
            finalState.mergePartialResult("total", row1);
            finalState.mergePartialResult("total", row2);

            assertEquals(ExprValueUtils.longValue(60L), finalState.finalResult());
        }
    }

    @Nested
    @DisplayName("AvgPartialAggState")
    class AvgTests {

        @Test
        @DisplayName("accumulate and produce final average")
        void accumulateAndFinalResult() {
            AvgPartialAggState state = new AvgPartialAggState();
            state.accumulate(ExprValueUtils.doubleValue(10.0));
            state.accumulate(ExprValueUtils.doubleValue(20.0));
            state.accumulate(ExprValueUtils.doubleValue(30.0));

            assertEquals(ExprValueUtils.doubleValue(20.0), state.finalResult());
        }

        @Test
        @DisplayName("empty avg returns null")
        void emptyAvgReturnsNull() {
            AvgPartialAggState state = new AvgPartialAggState();
            assertTrue(state.finalResult().isNull());
        }

        @Test
        @DisplayName("serialize and merge partial averages correctly")
        void serializeAndMerge() {
            // Partition 1: values [10, 20] → sum=30, count=2
            AvgPartialAggState partial1 = new AvgPartialAggState();
            partial1.accumulate(ExprValueUtils.doubleValue(10.0));
            partial1.accumulate(ExprValueUtils.doubleValue(20.0));

            Map<String, ExprValue> row1 = new LinkedHashMap<>();
            partial1.toPartialResult("avg_val", row1);
            assertEquals(ExprValueUtils.doubleValue(30.0), row1.get("avg_val_sum"));
            assertEquals(ExprValueUtils.longValue(2L), row1.get("avg_val_count"));

            // Partition 2: values [30] → sum=30, count=1
            AvgPartialAggState partial2 = new AvgPartialAggState();
            partial2.accumulate(ExprValueUtils.doubleValue(30.0));

            Map<String, ExprValue> row2 = new LinkedHashMap<>();
            partial2.toPartialResult("avg_val", row2);

            // Final merge: sum=60, count=3, avg=20
            AvgPartialAggState finalState = new AvgPartialAggState();
            finalState.mergePartialResult("avg_val", row1);
            finalState.mergePartialResult("avg_val", row2);

            assertEquals(ExprValueUtils.doubleValue(20.0), finalState.finalResult());
        }
    }

    @Nested
    @DisplayName("MinPartialAggState")
    class MinTests {

        @Test
        @DisplayName("accumulate and produce final minimum")
        void accumulateAndFinalResult() {
            MinPartialAggState state = new MinPartialAggState();
            state.accumulate(ExprValueUtils.integerValue(30));
            state.accumulate(ExprValueUtils.integerValue(10));
            state.accumulate(ExprValueUtils.integerValue(20));

            assertEquals(ExprValueUtils.integerValue(10), state.finalResult());
        }

        @Test
        @DisplayName("empty min returns null")
        void emptyMinReturnsNull() {
            MinPartialAggState state = new MinPartialAggState();
            assertTrue(state.finalResult().isNull());
        }

        @Test
        @DisplayName("serialize and merge partial minimums")
        void serializeAndMerge() {
            MinPartialAggState partial1 = new MinPartialAggState();
            partial1.accumulate(ExprValueUtils.integerValue(15));
            partial1.accumulate(ExprValueUtils.integerValue(25));

            Map<String, ExprValue> row1 = new LinkedHashMap<>();
            partial1.toPartialResult("min_v", row1);

            MinPartialAggState partial2 = new MinPartialAggState();
            partial2.accumulate(ExprValueUtils.integerValue(10));
            partial2.accumulate(ExprValueUtils.integerValue(50));

            Map<String, ExprValue> row2 = new LinkedHashMap<>();
            partial2.toPartialResult("min_v", row2);

            MinPartialAggState finalState = new MinPartialAggState();
            finalState.mergePartialResult("min_v", row1);
            finalState.mergePartialResult("min_v", row2);

            assertEquals(ExprValueUtils.integerValue(10), finalState.finalResult());
        }
    }

    @Nested
    @DisplayName("MaxPartialAggState")
    class MaxTests {

        @Test
        @DisplayName("accumulate and produce final maximum")
        void accumulateAndFinalResult() {
            MaxPartialAggState state = new MaxPartialAggState();
            state.accumulate(ExprValueUtils.integerValue(30));
            state.accumulate(ExprValueUtils.integerValue(10));
            state.accumulate(ExprValueUtils.integerValue(20));

            assertEquals(ExprValueUtils.integerValue(30), state.finalResult());
        }

        @Test
        @DisplayName("empty max returns null")
        void emptyMaxReturnsNull() {
            MaxPartialAggState state = new MaxPartialAggState();
            assertTrue(state.finalResult().isNull());
        }

        @Test
        @DisplayName("serialize and merge partial maximums")
        void serializeAndMerge() {
            MaxPartialAggState partial1 = new MaxPartialAggState();
            partial1.accumulate(ExprValueUtils.integerValue(15));
            partial1.accumulate(ExprValueUtils.integerValue(25));

            Map<String, ExprValue> row1 = new LinkedHashMap<>();
            partial1.toPartialResult("max_v", row1);

            MaxPartialAggState partial2 = new MaxPartialAggState();
            partial2.accumulate(ExprValueUtils.integerValue(10));
            partial2.accumulate(ExprValueUtils.integerValue(50));

            Map<String, ExprValue> row2 = new LinkedHashMap<>();
            partial2.toPartialResult("max_v", row2);

            MaxPartialAggState finalState = new MaxPartialAggState();
            finalState.mergePartialResult("max_v", row1);
            finalState.mergePartialResult("max_v", row2);

            assertEquals(ExprValueUtils.integerValue(50), finalState.finalResult());
        }
    }

    @Nested
    @DisplayName("AggregationType")
    class AggregationTypeTests {

        @Test
        @DisplayName("fromFunctionName resolves all supported types")
        void resolvesSupportedTypes() {
            assertEquals(AggregationType.COUNT, AggregationType.fromFunctionName("count"));
            assertEquals(AggregationType.SUM, AggregationType.fromFunctionName("sum"));
            assertEquals(AggregationType.AVG, AggregationType.fromFunctionName("avg"));
            assertEquals(AggregationType.MIN, AggregationType.fromFunctionName("min"));
            assertEquals(AggregationType.MAX, AggregationType.fromFunctionName("max"));
        }

        @Test
        @DisplayName("fromFunctionName is case-insensitive")
        void caseInsensitive() {
            assertEquals(AggregationType.COUNT, AggregationType.fromFunctionName("COUNT"));
            assertEquals(AggregationType.SUM, AggregationType.fromFunctionName("Sum"));
        }

        @Test
        @DisplayName("fromFunctionName throws for unsupported function")
        void throwsForUnsupported() {
            org.junit.jupiter.api.Assertions.assertThrows(
                    IllegalArgumentException.class,
                    () -> AggregationType.fromFunctionName("variance"));
        }
    }

    @Nested
    @DisplayName("PartialAggStateFactory")
    class FactoryTests {

        @Test
        @DisplayName("creates correct state types")
        void createsCorrectTypes() {
            assertTrue(
                    PartialAggStateFactory.create(AggregationType.COUNT, ExprCoreType.LONG)
                            instanceof CountPartialAggState);
            assertTrue(
                    PartialAggStateFactory.create(AggregationType.SUM, ExprCoreType.INTEGER)
                            instanceof SumPartialAggState);
            assertTrue(
                    PartialAggStateFactory.create(AggregationType.AVG, ExprCoreType.DOUBLE)
                            instanceof AvgPartialAggState);
            assertTrue(
                    PartialAggStateFactory.create(AggregationType.MIN, ExprCoreType.INTEGER)
                            instanceof MinPartialAggState);
            assertTrue(
                    PartialAggStateFactory.create(AggregationType.MAX, ExprCoreType.INTEGER)
                            instanceof MaxPartialAggState);
        }
    }
}
