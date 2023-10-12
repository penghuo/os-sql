/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.opensearch.sql.spark.execution.session.SessionType.INTERACTIVE;

import java.io.IOException;
import lombok.Builder;
import lombok.Data;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.index.seqno.SequenceNumbers;

@Data
@Builder
public class SessionModel implements ToXContentObject {
  public static final String VERSION = "version";
  public static final String TYPE = "type";
  public static final String SESSION_TYPE = "sessionType";
  public static final String SESSION_ID = "sessionId";
  public static final String SESSION_STATE = "state";
  public static final String DATASOURCE_NAME = "dataSourceName";

  public static final String SESSION_DOC_TYPE = "session";

  private final String version;
  private final SessionType sessionType;
  private final SessionId sessionID;
  private final SessionState sessionState;
  private final String datasourceName;

  private final long seqNo;
  private final long primaryTerm;

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    builder
        .startObject()
        .field(VERSION, version)
        .field(TYPE, SESSION_DOC_TYPE)
        .field(SESSION_TYPE, sessionType.getSessionType())
        .field(SESSION_ID, sessionID.getSessionId())
        .field(SESSION_STATE, sessionState)
        .field(DATASOURCE_NAME, datasourceName)
        .endObject();
    return builder;
  }

  public static SessionModel of(
      SessionModel copy, SessionState sessionState, long seqNo, long primaryTerm) {
    return builder()
        .version(copy.version)
        .sessionType(copy.sessionType)
        .sessionID(new SessionId(copy.sessionID.getSessionId()))
        .sessionState(sessionState)
        .datasourceName(copy.datasourceName)
        .seqNo(seqNo)
        .primaryTerm(primaryTerm)
        .build();
  }

  public static SessionModel of(SessionModel copy, long seqNo, long primaryTerm) {
    return builder()
        .version(copy.version)
        .sessionType(copy.sessionType)
        .sessionID(new SessionId(copy.sessionID.getSessionId()))
        .sessionState(copy.sessionState)
        .datasourceName(copy.datasourceName)
        .seqNo(seqNo)
        .primaryTerm(primaryTerm)
        .build();
  }

  public static SessionModel fromXContent(XContentParser parser, long seqNo, long primaryTerm) {
    try {
      SessionModelBuilder builder = new SessionModelBuilder();
      XContentParserUtils.ensureExpectedToken(
          XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
      while (!XContentParser.Token.END_OBJECT.equals(parser.nextToken())) {
        String fieldName = parser.currentName();
        parser.nextToken();
        switch (fieldName) {
          case VERSION:
            builder.version(parser.text());
            break;
          case SESSION_TYPE:
            builder.sessionType(SessionType.fromString(parser.text()));
            break;
          case SESSION_ID:
            builder.sessionID(new SessionId(parser.text()));
            break;
          case SESSION_STATE:
            builder.sessionState(SessionState.fromString(parser.text()));
            break;
          case DATASOURCE_NAME:
            builder.datasourceName(parser.text());
            break;
          case TYPE:
            // do nothing.
            break;
          default:
            throw new IllegalArgumentException("Unknown field " + fieldName);
        }
      }
      builder.seqNo(seqNo);
      builder.primaryTerm(primaryTerm);
      return builder.build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static SessionModel initInteractiveSession(SessionId sid, String datasourceName) {
    return new SessionModel(
        "1.0",
        INTERACTIVE,
        sid,
        SessionState.NOT_STARTED,
        datasourceName,
        SequenceNumbers.UNASSIGNED_SEQ_NO,
        SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
  }
}
