/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.benchmark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.util.BytesRef;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.ExceptionsHelper;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamOutput;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 1)
public class HashBenchmark {

  @Param({"1000", "10000", "100000"})
  public int iterations;

  private Random random = new Random();

  @Setup(Level.Invocation)
  public void setUp() {

  }

  @Benchmark
  public void encode(Blackhole bh) {
    for (int i = 0; i < iterations; i++) {
      bh.consume(encode(Arrays.asList(random.nextInt(32), random.nextInt(32))));
    }
  }

  @Benchmark
  public void hash(Blackhole bh) {
    for (int i = 0; i < iterations; i++) {
      bh.consume(Arrays.asList(random.nextInt(32), random.nextInt(32)).hashCode());
    }
  }

  private static BytesRef encode(List<Object> values) {
    try (BytesStreamOutput output = new BytesStreamOutput()) {
      output.writeCollection(values, StreamOutput::writeGenericValue);
      return output.bytes().toBytesRef();
    } catch (IOException e) {
      throw ExceptionsHelper.convertToRuntime(e);
    }
  }
}
