/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.execution.session.SessionType.INTERACTIVE;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.sql.spark.execution.statestore.StateModel;


@Data
@AllArgsConstructor
public class SessionModel {
  @Expose
  private String version;
  @Expose
  private SessionType sessionType;
  @Expose
  private SessionId sessionID;
  @Expose
  private SessionState sessionState;
  @Expose
  private String datasourceName;
  // occ

  private long seqNo;
  private long primaryTerm;

  public StateModel toStateModel() {
    try {
      XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
    } catch (IOException e) {

    }





    Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
    return new StateModel(sessionID.getSessionId(), gson.toJson(this), seqNo, primaryTerm);
  }

  public static SessionModel fromStateModel(XContentParser parser, final long seqNo,
                                            long primaryTerm) {
    return null;
  }

  public static SessionModel initInteractiveSession() {
    return new SessionModel("1.0", INTERACTIVE, new SessionId("0123456789012"),
        SessionState.NOT_STARTED,
        "dataSourceName", SequenceNumbers.UNASSIGNED_SEQ_NO,
        SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
  }
}
