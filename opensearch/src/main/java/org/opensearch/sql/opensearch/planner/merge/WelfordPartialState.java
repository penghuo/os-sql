/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.merge;

/**
 * Partial state for distributed STDDEV/VAR computation using Welford's online algorithm.
 *
 * <p>Each shard computes a partial state consisting of (count, mean, m2) where m2 is the sum of
 * squared deviations from the mean. Partial states from multiple shards are merged on the
 * coordinator using Chan's parallel algorithm, which is numerically stable even for large datasets
 * with values far from zero.
 *
 * <p>This approach avoids the catastrophic cancellation that occurs with the naive
 * {@code sum(x^2) - sum(x)^2/n} formula.
 *
 * @see <a href="https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm">
 *     Chan's parallel algorithm</a>
 */
public final class WelfordPartialState {

    private final long count;
    private final double mean;
    private final double m2;

    /**
     * Creates a WelfordPartialState with the given partial statistics.
     *
     * @param count the number of non-null values
     * @param mean the running mean
     * @param m2 the sum of squared deviations from the mean
     */
    public WelfordPartialState(long count, double mean, double m2) {
        this.count = count;
        this.mean = mean;
        this.m2 = m2;
    }

    /** Returns the count of values in this partial state. */
    public long getCount() {
        return count;
    }

    /** Returns the running mean of values in this partial state. */
    public double getMean() {
        return mean;
    }

    /** Returns the sum of squared deviations from the mean (M2). */
    public double getM2() {
        return m2;
    }

    /**
     * Merges two partial states using Chan's parallel algorithm.
     *
     * <p>Given two partial states (count_a, mean_a, m2_a) and (count_b, mean_b, m2_b), the combined
     * state is computed as:
     * <pre>
     *   count_combined = count_a + count_b
     *   delta = mean_b - mean_a
     *   mean_combined = (count_a * mean_a + count_b * mean_b) / count_combined
     *   m2_combined = m2_a + m2_b + delta^2 * count_a * count_b / count_combined
     * </pre>
     *
     * @param a the first partial state
     * @param b the second partial state
     * @return the merged partial state
     */
    public static WelfordPartialState merge(WelfordPartialState a, WelfordPartialState b) {
        if (a.count == 0) {
            return b;
        }
        if (b.count == 0) {
            return a;
        }

        long countCombined = a.count + b.count;
        double delta = b.mean - a.mean;
        double meanCombined =
                (a.count * a.mean + b.count * b.mean) / countCombined;
        double m2Combined =
                a.m2 + b.m2 + delta * delta * a.count * b.count / countCombined;

        return new WelfordPartialState(countCombined, meanCombined, m2Combined);
    }

    /**
     * Computes the population standard deviation from this partial state.
     *
     * @return sqrt(m2 / count), or NaN if count is 0
     */
    public double computeStddevPop() {
        if (count == 0) {
            return Double.NaN;
        }
        return Math.sqrt(m2 / count);
    }

    /**
     * Computes the sample standard deviation from this partial state.
     *
     * @return sqrt(m2 / (count - 1)), or NaN if count &lt;= 1
     */
    public double computeStddevSamp() {
        if (count <= 1) {
            return Double.NaN;
        }
        return Math.sqrt(m2 / (count - 1));
    }

    /**
     * Computes the population variance from this partial state.
     *
     * @return m2 / count, or NaN if count is 0
     */
    public double computeVarPop() {
        if (count == 0) {
            return Double.NaN;
        }
        return m2 / count;
    }

    /**
     * Computes the sample variance from this partial state.
     *
     * @return m2 / (count - 1), or NaN if count &lt;= 1
     */
    public double computeVarSamp() {
        if (count <= 1) {
            return Double.NaN;
        }
        return m2 / (count - 1);
    }

    /**
     * Builds a WelfordPartialState from raw values using Welford's online algorithm.
     *
     * <p>This is primarily useful for testing and for building partial states from shard-local data.
     *
     * @param values the raw data values
     * @return a WelfordPartialState representing the statistics of the given values
     */
    public static WelfordPartialState fromValues(double... values) {
        long n = 0;
        double mean = 0.0;
        double m2 = 0.0;

        for (double x : values) {
            n++;
            double delta = x - mean;
            mean += delta / n;
            double delta2 = x - mean;
            m2 += delta * delta2;
        }

        return new WelfordPartialState(n, mean, m2);
    }

    @Override
    public String toString() {
        return String.format(
                "WelfordPartialState{count=%d, mean=%.6f, m2=%.6f}", count, mean, m2);
    }
}
