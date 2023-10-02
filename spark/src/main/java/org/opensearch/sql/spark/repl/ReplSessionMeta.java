/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.repl;

import static org.opensearch.sql.spark.repl.ReplSessionState.STANDBY;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@EqualsAndHashCode
@JsonIgnoreProperties(ignoreUnknown = true)
public class ReplSessionMeta {
  @JsonProperty
  private String type;

  @JsonProperty
  private String sessionId;

  @JsonProperty
  private String applicationId;

  @JsonProperty
  private String jobId;

  @JsonProperty
  private ReplSessionState state;

  public static ReplSessionMeta standBy(String sessionId) {
    return new ReplSessionMeta("job", sessionId, "unknown", "unknown", STANDBY);
  }
}
