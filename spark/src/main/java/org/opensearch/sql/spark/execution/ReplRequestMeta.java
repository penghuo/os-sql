/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution;

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
public class ReplRequestMeta {
  @JsonProperty
  private String type;

  @JsonProperty
  private String sessionId;

  @JsonProperty
  private String queryId;

  @JsonProperty
  private String query;

  @JsonProperty
  private ReplRequestState state;

  @JsonProperty
  private Long submitTime;

  public static ReplRequestMeta init(String sessionId, String queryId, String query) {
    return new ReplRequestMeta("request", sessionId, queryId, query, ReplRequestState.PENDING,
        System.currentTimeMillis());
  }
}
