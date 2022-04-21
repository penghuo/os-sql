/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.benchmark;

import com.google.common.collect.ImmutableMap;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.opensearch.operator.common.SerDe;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(value = 1, warmups = 1)
@Warmup(iterations = 1)
public class SerDeBenchmark {
  @Param({ "100", "1000", "10000"})
//  @Param({ "100"})
  public int iterations;

  private Random ran = new Random();
  private RootAllocator allocator;

  @Setup(Level.Invocation)
  public void setUp() {
    ran = new Random();
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @TearDown(Level.Invocation)
  public void clean() {
    allocator.close();
  }

//  @org.openjdk.jmh.annotations.Benchmark
//  public void ser_bindingTuple(SerDeBenchmark bh, Blackhole blackhole) {
//    List<ExprValue> tuples = new ArrayList<>();
//    for (int i = 0; i < bh.iterations; i++) {
//      tuples.add(ExprValueUtils.tupleValue(
//          ImmutableMap.of("integer", ran.nextInt(Integer.MAX_VALUE))));
//    }
//    blackhole.consume(SerDe.serializeExprValue(new ExprCollectionValue(tuples)));
//  }

  @org.openjdk.jmh.annotations.Benchmark
  public void serde_bindingTuple(SerDeBenchmark bh, Blackhole blackhole) {
    List<ExprValue> tuples = new ArrayList<>();
    for (int i = 0; i < bh.iterations; i++) {
      tuples.add(ExprValueUtils.tupleValue(
          ImmutableMap.of("integer", ran.nextInt(Integer.MAX_VALUE))));
    }
    String data = SerDe.serializeExprValue(new ExprCollectionValue(tuples));

    blackhole.consume(SerDe.deserializeExprValue(data));
  }

//  @org.openjdk.jmh.annotations.Benchmark
//  public void ser_arrow(SerDeBenchmark bh, Blackhole blackhole) throws IOException {
//    int N = bh.iterations;
//    IntVector intVector = new IntVector("integer", allocator);
//    intVector.allocateNew(N);
//    for (int i = 0; i < N; i++) {
//      intVector.set(i, ran.nextInt(Integer.MAX_VALUE));
//    }
//    intVector.setValueCount(N);
//
//    List<Field> fields = Arrays.asList(intVector.getField());
//    List<FieldVector> vectors = Arrays.asList(intVector);
//    VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);
//
//    ByteArrayOutputStream out = new ByteArrayOutputStream();
//    ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out));
//    writer.start();
//    writer.writeBatch();
//    writer.end();
//    blackhole.consume(out.toString());
//    intVector.clear();
//  }

  @org.openjdk.jmh.annotations.Benchmark
  public void serde_arrow(SerDeBenchmark bh, Blackhole blackhole) throws IOException {
    int N = bh.iterations;
    IntVector intVector = new IntVector("integer", allocator);
    intVector.allocateNew(N);
    for (int i = 0; i < N; i++) {
      intVector.set(i, ran.nextInt(Integer.MAX_VALUE));
    }
    intVector.setValueCount(N);

    List<Field> fields = Arrays.asList(intVector.getField());
    List<FieldVector> vectors = Arrays.asList(intVector);
    VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out));
    writer.start();
    writer.writeBatch();
    writer.end();
    final byte[] bytes = out.toByteArray();
    intVector.clear();

    try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
      reader.loadNextBatch();
      VectorSchemaRoot readRoot = reader.getVectorSchemaRoot();
      blackhole.consume(readRoot);
    }
  }
}
