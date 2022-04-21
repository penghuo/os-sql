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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
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
  @Param({"1000", "10000", "100000"})
//  @Param({ "100"})
  public int iterations;

  private Random ran = new Random();
  private RootAllocator allocator;
  IntVector intVector;
  BigIntVector bigIntVector;
  VarCharVector varCharVector;
  Container container;


  @Setup(Level.Invocation)
  public void setUp() {
    ran = new Random();
    allocator = new RootAllocator(Long.MAX_VALUE);

    int N = iterations;
    intVector = new IntVector("integer", allocator);
    bigIntVector = new BigIntVector("long", allocator);
    varCharVector = new VarCharVector("varchar", allocator);
    intVector.allocateNew(N);
    varCharVector.allocateNew(N);
    bigIntVector.allocateNew(N);
    for (int i = 0; i < N; i++) {
      final int v = i;//ran.nextInt(Integer.MAX_VALUE);
      intVector.set(i, v);
      bigIntVector.set(i, ran.nextLong());
      varCharVector.setSafe(i, ("test" + v).getBytes(StandardCharsets.UTF_8));
    }
    intVector.setValueCount(N);
    varCharVector.setValueCount(N);
    bigIntVector.setValueCount(N);

    List<InternalField> fieldList = new ArrayList<>(N);
    for (int i = 0; i < N; i++) {
      final int v = i;//ran.nextInt(Integer.MAX_VALUE);
      fieldList.add(new InternalField(i, ran.nextLong(),
          ("test" + v).getBytes(StandardCharsets.UTF_8)));
    }
    container =  new Container(fieldList);
  }

  @TearDown(Level.Invocation)
  public void clean() {
    intVector.clear();
    bigIntVector.clear();
    varCharVector.clear();

    allocator.close();
  }

//  @Benchmark
//  public void ser_bindingTuple(SerDeBenchmark bh, Blackhole blackhole) {
//    List<ExprValue> tuples = new ArrayList<>();
//    for (int i = 0; i < bh.iterations; i++) {
//      tuples.add(ExprValueUtils.tupleValue(
//          ImmutableMap.of("integer", ran.nextInt(Integer.MAX_VALUE))));
//    }
//    blackhole.consume(SerDe.serializeExprValue(new ExprCollectionValue(tuples)));
//  }

//  @Benchmark
//  public void serde_bindingTuple(SerDeBenchmark bh, Blackhole blackhole) {
//    List<ExprValue> tuples = new ArrayList<>();
//    for (int i = 0; i < bh.iterations; i++) {
//      tuples.add(ExprValueUtils.tupleValue(
//          ImmutableMap.of("integer", ran.nextInt(Integer.MAX_VALUE))));
//    }
//    String data = SerDe.serializeExprValue(new ExprCollectionValue(tuples));
//
//    blackhole.consume(SerDe.deserializeExprValue(data));
//  }

//  @Benchmark
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

//  @Benchmark
  public void serde_arrow(SerDeBenchmark bh, Blackhole blackhole) throws IOException {
    int N = bh.iterations;
    IntVector intVector = new IntVector("integer", allocator);
    intVector.allocateNew(N);
    for (int i = 0; i < N; i++) {
      final int v = ran.nextInt(Integer.MAX_VALUE);
      intVector.set(i, v);
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

  @Benchmark
  public void serde_arrow_multi(SerDeBenchmark bh, Blackhole blackhole) throws IOException {
    List<Field> fields = Arrays.asList(intVector.getField(),
        bigIntVector.getField(), varCharVector.getField());
    List<FieldVector> vectors = Arrays.asList(intVector, bigIntVector, varCharVector);
    VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out));
    writer.start();
    writer.writeBatch();
    writer.end();
    byte[] bytes = out.toByteArray();

    try (ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
      reader.loadNextBatch();
      VectorSchemaRoot readRoot = reader.getVectorSchemaRoot();
      blackhole.consume(readRoot);
    }
  }

//  @Benchmark
  public void serde_stream(SerDeBenchmark bh, Blackhole blackhole) throws IOException {
    final BytesStreamOutput output = new BytesStreamOutput();
    container.writeTo(output);
    output.flush();
    byte[] bytes = BytesReference.toBytes(output.bytes());
    output.close();

    final BytesStreamInput input = new BytesStreamInput(bytes);
    Container container = new Container(input);
    blackhole.consume(container);
  }

  @Benchmark
  public void serde_stream_multi(SerDeBenchmark bh, Blackhole blackhole) throws IOException {
    BytesStreamOutput output = new BytesStreamOutput();
    container.writeTo(output);
    output.flush();
    byte[] bytes = BytesReference.toBytes(output.bytes());
    output.close();

    BytesStreamInput input = new BytesStreamInput(bytes);
    Container container = new Container(input);
    blackhole.consume(container);
    input.close();
  }

  static class Container implements Writeable {
    List<InternalField> fields;

    public Container(List<InternalField> fields) {
      this.fields = fields;
    }

    public Container(StreamInput in) throws IOException {
      this.fields = in.readList(InternalField::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
      out.writeList(fields);
    }

    @Override
    public String toString() {
      return "Container{" +
          "fields=" + fields +
          '}';
    }
  }

  static class InternalField implements Writeable {
    int i;
    long l;
    byte[] s;

    public InternalField(int i, long l, byte[] s) {
      this.i = i;
      this.l = l;
      this.s = s;
    }

    public InternalField(StreamInput in) throws IOException {
      this.i = in.readInt();
      this.l = in.readLong();
      this.s = in.readByteArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
      out.writeInt(i);
      out.writeLong(l);
      out.writeByteArray(s);
    }

    @Override
    public String toString() {
      return "InternalField{" +
          "i=" + i +
          ", s='" + s + '\'' +
          '}';
    }
  }
}
