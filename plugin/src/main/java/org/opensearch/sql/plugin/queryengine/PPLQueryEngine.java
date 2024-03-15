package org.opensearch.sql.plugin.queryengine;

import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.search.TotalHits;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.externalengine.QueryEngine;
import org.opensearch.search.externalengine.QueryEngineExtBuilder;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.JdbcResponseFormatter;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;

public class PPLQueryEngine extends QueryEngine {

  public static final String NAME = "ppl";
  private static PPLService pplService;
  private String query;
  private String format;

  public static void initialize(PPLService pplService) {
    PPLQueryEngine.pplService = pplService;
  }

  @Override
  public void executeQuery(
      SearchRequest searchRequest, ActionListener<SearchResponse> actionListener) {
    PPLQueryRequest pplQueryRequest = new PPLQueryRequest(query, null, "_search", format);
    pplService.execute(
        pplQueryRequest,
        new ResponseListener<>() {
          @Override
          public void onResponse(ExecutionEngine.QueryResponse queryResponse) {
            SearchResponse searchResponse =
                transformFromQueryResponseToSearchResponse(queryResponse);
            actionListener.onResponse(searchResponse);
          }

          @Override
          public void onFailure(Exception e) {
            actionListener.onFailure(e);
          }
        });
  }

  private SearchResponse transformFromQueryResponseToSearchResponse(
      ExecutionEngine.QueryResponse queryResponse) {
    SearchHit[] hits = new SearchHit[0];
    return new SearchResponse(
        new InternalSearchResponse(
            new SearchHits(hits, new TotalHits(0L, TotalHits.Relation.EQUAL_TO), 0.0F),
            (InternalAggregations) null,
            null,
            null,
            false,
            (Boolean) null,
            1,
            Collections.emptyList(),
            List.of(new PPLQueryEngine.PPLResponseExternalBuilder(queryResponse))),
        (String) null,
        0,
        0,
        0,
        0L,
        ShardSearchFailure.EMPTY_ARRAY,
        SearchResponse.Clusters.EMPTY,
        null);
  }

  static class PPLResponseExternalBuilder extends QueryEngineExtBuilder {

    static ParseField DUMMY_FIELD = new ParseField("ppl");

    protected final ExecutionEngine.QueryResponse queryResponse;

    protected final QueryResult queryResult;

    public PPLResponseExternalBuilder(ExecutionEngine.QueryResponse queryResponse) {
      this.queryResponse = queryResponse;
      this.queryResult = new QueryResult(queryResponse.getSchema(), queryResponse.getResults(),
          queryResponse.getCursor());
    }

    public PPLResponseExternalBuilder(StreamInput in) throws IOException {
      this.queryResponse = null;
      this.queryResult =  null;
    }

    @Override
    public String getWriteableName() {
      return DUMMY_FIELD.getPreferredName();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
      out.writeString("1");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
      SimpleJsonResponseFormatter formatter =
          new SimpleJsonResponseFormatter(JsonResponseFormatter.Style.PRETTY);
      String result = formatter.format(queryResult);
      JSONObject jsonObject = new JSONObject(result);

      builder.startObject(NAME);
      builder.startArray("schema");
      JSONArray schema = jsonObject.getJSONArray("schema");
      for (int i = 0; i < schema.length(); i++) {
        JSONObject column = schema.getJSONObject(i);
        builder.startObject();
        builder.field("name", column.getString("name"));
        builder.field("type", column.getString("type"));
        builder.endObject();
      }
      builder.endArray();

      builder.startArray("datarows");
      JSONArray datarows = jsonObject.getJSONArray("datarows");
      for (int i = 0; i < datarows.length(); i++) {
        builder.startArray();
        JSONArray row = datarows.getJSONArray(i);
        for (int j = 0; j < row.length(); j++) {
          if (row.isNull(j)) {
            builder.nullValue();
          } else {
            Object o = row.get(j);
            if (o instanceof JSONObject) {
              jsonObject(builder, (JSONObject) o);
            } else if (o instanceof JSONArray) {
              jsonArray(builder, (JSONArray) o);
            } else {
              builder.value(o);
            }
          }
        }
        builder.endArray();
      }
      builder.endArray();

      builder.field("total", jsonObject.getLong("total"));
      builder.field("size", jsonObject.getLong("size"));
      builder.endObject();

//      // Serialize the schema
//      builder.startObject(NAME);
//      ArrayList<String> columnNames = new ArrayList<>();
//      builder.startArray("schema");
//      for (ExecutionEngine.Schema.Column column : queryResponse.getSchema().getColumns()) {
//        builder.startObject();
//        String columnName = getColumnName(column);
//        columnNames.add(columnName);
//        builder.field("name", columnName);
//        builder.field("type", column.getExprType().typeName().toLowerCase(Locale.ROOT));
//        builder.endObject();
//      }
//      builder.endArray();
//      builder.startArray("datarows");
//      for (Object[] values : queryResult) {
//        builder.startArray();
//        for (Object value : values) {
//          builder.value(value);
//        }
//        builder.endArray();
//      }
//      builder.endArray();
//      builder.field("total", queryResponse.getResults().size());
//      builder.field("size", queryResponse.getResults().size());
//      builder.endObject();
      return builder;
    }

    public XContentBuilder jsonObject(XContentBuilder builder, JSONObject object) throws IOException {
      Map<String, Object> objectMap = object.toMap();
      builder.startObject();
      for (String key : objectMap.keySet()) {
        Object value = objectMap.get(key);
        if (value instanceof JSONObject) {
          jsonObject(builder, (JSONObject) value);
        } else if (value instanceof JSONArray) {
          jsonArray(builder, (JSONArray) value);
        } else {
          builder.field(key, value);
        }
      }
      builder.endObject();
      return builder;
    }

    public XContentBuilder jsonArray(XContentBuilder builder, JSONArray array) throws IOException {
      builder.startArray();
      for (int i = 0; i < array.length(); i++) {
        Object value = array.get(i);
        if (value instanceof JSONObject) {
          jsonObject(builder, (JSONObject) value);
        } else if (value instanceof JSONArray) {
          jsonArray(builder, (JSONArray) value);
        } else {
          builder.value(value);
        }
      }
      builder.endArray();
      return builder;
    }

    @Override
    public int hashCode() {
      return 0;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      return true;
    }

    public static PPLQueryEngine.PPLResponseExternalBuilder parse(XContentParser parser)
        throws IOException {
      return null;
    }

    private String getColumnName(ExecutionEngine.Schema.Column column) {
      return (column.getAlias() != null) ? column.getAlias() : column.getName();
    }

  }

  public PPLQueryEngine(String query, String format) {
    this.query = query;
    this.format = format;
  }

  public PPLQueryEngine(StreamInput in) {}

  @Override
  public String getWriteableName() {
    return NAME;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {}

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    return null;
  }

  public static QueryEngine fromXContent(XContentParser parser) throws IOException {
    XContentParser.Token token;
    String query = "";
    String format = "json";
    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
      String fieldName = parser.currentName();
      token = parser.nextToken();
      if (fieldName.equals("query")) {
        query = parser.textOrNull();
      }
      if (fieldName.equals("format")) {
        format = parser.textOrNull();
      }
    }
    return new PPLQueryEngine(query, format);
  }


}
