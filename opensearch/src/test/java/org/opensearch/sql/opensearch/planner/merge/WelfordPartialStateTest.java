/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.merge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class WelfordPartialStateTest {

    private static final double EPSILON = 1e-10;

    @Nested
    @DisplayName("fromValues factory")
    class FromValuesTests {

        @Test
        @DisplayName("builds correct state from multiple values")
        void testFromMultipleValues() {
            WelfordPartialState state = WelfordPartialState.fromValues(2.0, 4.0, 6.0, 8.0);
            assertEquals(4, state.getCount());
            assertEquals(5.0, state.getMean(), EPSILON);
            // M2 = (2-5)^2 + (4-5)^2 + (6-5)^2 + (8-5)^2 = 9+1+1+9 = 20
            assertEquals(20.0, state.getM2(), EPSILON);
        }

        @Test
        @DisplayName("builds correct state from single value")
        void testFromSingleValue() {
            WelfordPartialState state = WelfordPartialState.fromValues(42.0);
            assertEquals(1, state.getCount());
            assertEquals(42.0, state.getMean(), EPSILON);
            assertEquals(0.0, state.getM2(), EPSILON);
        }

        @Test
        @DisplayName("builds empty state from no values")
        void testFromNoValues() {
            WelfordPartialState state = WelfordPartialState.fromValues();
            assertEquals(0, state.getCount());
        }

        @Test
        @DisplayName("handles all same values")
        void testAllSameValues() {
            WelfordPartialState state = WelfordPartialState.fromValues(5.0, 5.0, 5.0, 5.0);
            assertEquals(4, state.getCount());
            assertEquals(5.0, state.getMean(), EPSILON);
            assertEquals(0.0, state.getM2(), EPSILON);
        }
    }

    @Nested
    @DisplayName("variance and stddev computations")
    class ComputationTests {

        @Test
        @DisplayName("population variance for known dataset")
        void testVarPop() {
            // {2, 4, 6, 8}: mean=5, varPop = 20/4 = 5.0
            WelfordPartialState state = WelfordPartialState.fromValues(2.0, 4.0, 6.0, 8.0);
            assertEquals(5.0, state.computeVarPop(), EPSILON);
        }

        @Test
        @DisplayName("sample variance for known dataset")
        void testVarSamp() {
            // {2, 4, 6, 8}: varSamp = 20/3 = 6.666...
            WelfordPartialState state = WelfordPartialState.fromValues(2.0, 4.0, 6.0, 8.0);
            assertEquals(20.0 / 3.0, state.computeVarSamp(), EPSILON);
        }

        @Test
        @DisplayName("population stddev for known dataset")
        void testStddevPop() {
            WelfordPartialState state = WelfordPartialState.fromValues(2.0, 4.0, 6.0, 8.0);
            assertEquals(Math.sqrt(5.0), state.computeStddevPop(), EPSILON);
        }

        @Test
        @DisplayName("sample stddev for known dataset")
        void testStddevSamp() {
            WelfordPartialState state = WelfordPartialState.fromValues(2.0, 4.0, 6.0, 8.0);
            assertEquals(Math.sqrt(20.0 / 3.0), state.computeStddevSamp(), EPSILON);
        }

        @Test
        @DisplayName("NaN for empty dataset")
        void testEmptyDataset() {
            WelfordPartialState state = WelfordPartialState.fromValues();
            assertTrue(Double.isNaN(state.computeVarPop()));
            assertTrue(Double.isNaN(state.computeVarSamp()));
            assertTrue(Double.isNaN(state.computeStddevPop()));
            assertTrue(Double.isNaN(state.computeStddevSamp()));
        }

        @Test
        @DisplayName("NaN for sample variance/stddev with single value")
        void testSingleValueSample() {
            WelfordPartialState state = WelfordPartialState.fromValues(10.0);
            assertEquals(0.0, state.computeVarPop(), EPSILON);
            assertEquals(0.0, state.computeStddevPop(), EPSILON);
            assertTrue(Double.isNaN(state.computeVarSamp()));
            assertTrue(Double.isNaN(state.computeStddevSamp()));
        }
    }

    @Nested
    @DisplayName("merge via Chan's parallel algorithm")
    class MergeTests {

        @Test
        @DisplayName("merge two non-overlapping partitions produces correct variance")
        void testMergeTwoPartitions() {
            WelfordPartialState left = WelfordPartialState.fromValues(2.0, 4.0);
            WelfordPartialState right = WelfordPartialState.fromValues(6.0, 8.0);
            WelfordPartialState merged = WelfordPartialState.merge(left, right);

            assertEquals(4, merged.getCount());
            assertEquals(5.0, merged.getMean(), EPSILON);
            assertEquals(20.0, merged.getM2(), EPSILON);
        }

        @Test
        @DisplayName("merge with empty partition returns non-empty partition")
        void testMergeWithEmpty() {
            WelfordPartialState empty = WelfordPartialState.fromValues();
            WelfordPartialState data = WelfordPartialState.fromValues(1.0, 2.0, 3.0);

            WelfordPartialState merged1 = WelfordPartialState.merge(empty, data);
            assertEquals(data.getCount(), merged1.getCount());
            assertEquals(data.getMean(), merged1.getMean(), EPSILON);
            assertEquals(data.getM2(), merged1.getM2(), EPSILON);

            WelfordPartialState merged2 = WelfordPartialState.merge(data, empty);
            assertEquals(data.getCount(), merged2.getCount());
            assertEquals(data.getMean(), merged2.getMean(), EPSILON);
            assertEquals(data.getM2(), merged2.getM2(), EPSILON);
        }

        @Test
        @DisplayName("merge multiple partitions matches single-pass result")
        void testMergeMultiplePartitions() {
            // Split {1, 2, 3, 4, 5, 6, 7, 8, 9, 10} into three partitions
            WelfordPartialState p1 = WelfordPartialState.fromValues(1.0, 2.0, 3.0);
            WelfordPartialState p2 = WelfordPartialState.fromValues(4.0, 5.0, 6.0, 7.0);
            WelfordPartialState p3 = WelfordPartialState.fromValues(8.0, 9.0, 10.0);

            WelfordPartialState merged = WelfordPartialState.merge(
                    WelfordPartialState.merge(p1, p2), p3);

            WelfordPartialState reference = WelfordPartialState.fromValues(
                    1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0);

            assertEquals(reference.getCount(), merged.getCount());
            assertEquals(reference.getMean(), merged.getMean(), EPSILON);
            assertEquals(reference.getM2(), merged.getM2(), EPSILON);
            assertEquals(reference.computeStddevPop(), merged.computeStddevPop(), EPSILON);
            assertEquals(reference.computeStddevSamp(), merged.computeStddevSamp(), EPSILON);
        }

        @Test
        @DisplayName("merge is consistent regardless of partition order")
        void testMergeOrderIndependence() {
            WelfordPartialState a = WelfordPartialState.fromValues(10.0, 20.0);
            WelfordPartialState b = WelfordPartialState.fromValues(30.0, 40.0);

            WelfordPartialState ab = WelfordPartialState.merge(a, b);
            WelfordPartialState ba = WelfordPartialState.merge(b, a);

            assertEquals(ab.getCount(), ba.getCount());
            assertEquals(ab.getMean(), ba.getMean(), EPSILON);
            assertEquals(ab.getM2(), ba.getM2(), EPSILON);
        }

        @Test
        @DisplayName("merge single-element partitions")
        void testMergeSingleElements() {
            WelfordPartialState a = WelfordPartialState.fromValues(3.0);
            WelfordPartialState b = WelfordPartialState.fromValues(7.0);
            WelfordPartialState merged = WelfordPartialState.merge(a, b);

            assertEquals(2, merged.getCount());
            assertEquals(5.0, merged.getMean(), EPSILON);
            // M2 = (3-5)^2 + (7-5)^2 = 4 + 4 = 8
            assertEquals(8.0, merged.getM2(), EPSILON);
        }
    }

    @Test
    @DisplayName("toString produces readable representation")
    void testToString() {
        WelfordPartialState state = WelfordPartialState.fromValues(1.0, 2.0, 3.0);
        String str = state.toString();
        assertTrue(str.contains("count=3"));
        assertTrue(str.contains("mean="));
        assertTrue(str.contains("m2="));
    }
}
