/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.sql.calcite;

import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory that creates an {@link OpenSearchSchema}.
 *
 * <p>Allows a custom schema to be included in a model.json file.
 */
@SuppressWarnings("UnusedDeclaration")
public class OpenSearchSchemaFactory implements SchemaFactory {

  private static final Logger LOGGER = LoggerFactory.getLogger(OpenSearchSchemaFactory.class);

  private static final int REST_CLIENT_CACHE_SIZE = 100;

  public OpenSearchSchemaFactory() {
  }

  /**
   * Create an OpenSearch {@link Schema}.
   * The operand property accepts the following key/value pairs:
   *
   * <ul>
   *   <li><b>username</b>: The username for the ES cluster</li>
   *   <li><b>password</b>: The password for the ES cluster</li>
   *   <li><b>hosts</b>: A {@link List} of hosts for the ES cluster. Either the hosts or
   *   coordinates must be populated.</li>
   *   <li><b>coordinates</b>: A {@link List} of coordinates for the ES cluster. Either the hosts
   *   list or
   *   the coordinates list must be populated.</li>
   *   <li><b>disableSSLVerification</b>: A boolean parameter to disable SSL verification. Defaults
   *   to false. This should always be set to false for production systems.</li>
   * </ul>
   *
   * @param parentSchema Parent schema
   * @param name Name of this schema
   * @param operand The "operand" JSON property
   * @return Returns a {@link Schema} for the ES cluster.
   */
  @Override public Schema create(SchemaPlus parentSchema, String name,
      Map<String, Object> operand) {
    throw new UnsupportedOperationException("create Schema failed");
  }
}
