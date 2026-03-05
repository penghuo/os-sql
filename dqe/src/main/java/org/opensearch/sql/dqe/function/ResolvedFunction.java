/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.function;

import io.trino.spi.type.Type;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.sql.dqe.common.types.TypeSignatures;

/**
 * A serializable reference to a resolved function. Contains enough information to re-resolve the
 * function implementation from a {@link FunctionRegistry} after deserialization on a different
 * node.
 */
@Getter
public class ResolvedFunction implements Writeable {

  private final String name;
  private final List<Type> argumentTypes;
  private final Type returnType;
  private final FunctionKind kind;

  public ResolvedFunction(
      String name, List<Type> argumentTypes, Type returnType, FunctionKind kind) {
    this.name = name;
    this.argumentTypes = List.copyOf(argumentTypes);
    this.returnType = returnType;
    this.kind = kind;
  }

  /** Deserialize from a stream. */
  public ResolvedFunction(StreamInput in) throws IOException {
    this.name = in.readString();
    List<String> argSignatures = in.readStringList();
    this.argumentTypes =
        argSignatures.stream().map(TypeSignatures::fromSignature).collect(Collectors.toList());
    this.returnType = TypeSignatures.fromSignature(in.readString());
    this.kind = FunctionKind.valueOf(in.readString());
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(name);
    out.writeStringCollection(
        argumentTypes.stream().map(TypeSignatures::toSignature).collect(Collectors.toList()));
    out.writeString(TypeSignatures.toSignature(returnType));
    out.writeString(kind.name());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ResolvedFunction that)) return false;
    return Objects.equals(name, that.name)
        && Objects.equals(argumentTypes, that.argumentTypes)
        && kind == that.kind;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, argumentTypes, kind);
  }

  @Override
  public String toString() {
    return name
        + "("
        + argumentTypes.stream().map(Type::getDisplayName).collect(Collectors.joining(", "))
        + ") -> "
        + returnType.getDisplayName();
  }
}
