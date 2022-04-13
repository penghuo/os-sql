/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.operator.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.physical.PhysicalPlan;

public class SerDe {
  public static String serialize(PhysicalPlan expr) {
    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      ObjectOutputStream objectOutput = new ObjectOutputStream(output);
      objectOutput.writeObject(expr);
      objectOutput.flush();
      return Base64.getEncoder().encodeToString(output.toByteArray());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize expression: " + expr, e);
    }
  }

  public static PhysicalPlan deserialize(String code) {
    try {
      ByteArrayInputStream input = new ByteArrayInputStream(Base64.getDecoder().decode(code));
      ObjectInputStream objectInput = new ObjectInputStream(input);
      return (PhysicalPlan) objectInput.readObject();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to deserialize expression code: " + code, e);
    }
  }

  public static String serializeExprValue(ExprValue expr) {
    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      ObjectOutputStream objectOutput = new ObjectOutputStream(output);
      objectOutput.writeObject(expr);
      objectOutput.flush();
      return Base64.getEncoder().encodeToString(output.toByteArray());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to serialize expression: " + expr, e);
    }
  }

  public static ExprValue deserializeExprValue(String code) {
    try {
      ByteArrayInputStream input = new ByteArrayInputStream(Base64.getDecoder().decode(code));
      ObjectInputStream objectInput = new ObjectInputStream(input);
      return (ExprValue) objectInput.readObject();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to deserialize expression code: " + code, e);
    }
  }
}
