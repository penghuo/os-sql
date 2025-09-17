/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.util;

import static org.opensearch.sql.data.model.ExprValueUtils.LITERAL_NULL;
import static org.opensearch.sql.data.model.ExprValueUtils.booleanValue;
import static org.opensearch.sql.data.model.ExprValueUtils.byteValue;
import static org.opensearch.sql.data.model.ExprValueUtils.collectionValue;
import static org.opensearch.sql.data.model.ExprValueUtils.dateValue;
import static org.opensearch.sql.data.model.ExprValueUtils.doubleValue;
import static org.opensearch.sql.data.model.ExprValueUtils.floatValue;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.intervalValue;
import static org.opensearch.sql.data.model.ExprValueUtils.longValue;
import static org.opensearch.sql.data.model.ExprValueUtils.shortValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.model.ExprValueUtils.timeValue;
import static org.opensearch.sql.data.model.ExprValueUtils.timestampValue;
import static org.opensearch.sql.data.model.ExprValueUtils.tupleValue;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAmount;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.apache.calcite.avatica.util.ArrayImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.locationtech.jts.geom.Point;
import org.opensearch.sql.calcite.type.ExprJavaType;
import org.opensearch.sql.data.model.ExprCollectionValue;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprGeoPointValue;

/** This class is used to convert the data type from JDBC to OpenSearch data type. */
@UtilityClass
public class JdbcOpenSearchDataTypeConvertor {
  private static final Logger LOG = LogManager.getLogger();

  public static ExprType getExprTypeFromSqlType(int sqlType) {
    switch (sqlType) {
      case Types.INTEGER:
        return ExprCoreType.INTEGER;
      case Types.BIGINT:
        return ExprCoreType.LONG;
      case Types.DOUBLE:
      case Types.DECIMAL:
      case Types.NUMERIC:
        return ExprCoreType.DOUBLE;
      case Types.FLOAT:
        return ExprCoreType.FLOAT;
      case Types.DATE:
        return ExprCoreType.DATE;
      case Types.TIMESTAMP:
        return ExprCoreType.TIMESTAMP;
      case Types.BOOLEAN:
        return ExprCoreType.BOOLEAN;
      case Types.VARCHAR:
      case Types.CHAR:
      case Types.LONGVARCHAR:
        return ExprCoreType.STRING;
      default:
        // TODO unchecked OpenSearchDataType
        return ExprCoreType.UNKNOWN;
    }
  }

  public static ExprValue getExprValueFromSqlType(
      ResultSet rs, int i, int sqlType, RelDataType fieldType, String fieldName)
      throws SQLException {
    Object value = rs.getObject(i);
    if (value == null) {
      return ExprNullValue.of();
    }

    if (fieldType instanceof ExprJavaType && value instanceof ExprValue) {
      return (ExprValue) value;
    } else if (fieldType.getSqlTypeName() == SqlTypeName.GEOMETRY) {
      // Use getObject by name instead of index to avoid Avatica's transformation on the accessor.
      // Otherwise, Avatica will transform Geometry to String.
      Point geoPoint = (Point) rs.getObject(fieldName);
      return new OpenSearchExprGeoPointValue(geoPoint.getY(), geoPoint.getX());
    }

    try {
      switch (sqlType) {
        case Types.VARCHAR:
        case Types.CHAR:
        case Types.LONGVARCHAR:
          return ExprValueUtils.fromObjectValue(rs.getString(i));

        case Types.INTEGER:
          return ExprValueUtils.fromObjectValue(rs.getInt(i));

        case Types.BIGINT:
          return ExprValueUtils.fromObjectValue(rs.getLong(i));

        case Types.FLOAT:
        case Types.REAL:
          return ExprValueUtils.fromObjectValue(rs.getFloat(i));

        case Types.DECIMAL:
        case Types.NUMERIC:
        case Types.DOUBLE:
          return ExprValueUtils.fromObjectValue(rs.getDouble(i));

        case Types.DATE:
          String dateStr = rs.getString(i);
          return new ExprDateValue(dateStr);

        case Types.TIME:
          String timeStr = rs.getString(i);
          return new ExprTimeValue(timeStr);

        case Types.TIMESTAMP:
          String timestampStr = rs.getString(i);
          return new ExprTimestampValue(timestampStr);

        case Types.BOOLEAN:
          return ExprValueUtils.fromObjectValue(rs.getBoolean(i));

        case Types.ARRAY:
          Array array = rs.getArray(i);
          if (array instanceof ArrayImpl) {
            return ExprValueUtils.fromObjectValue(
                Arrays.asList((Object[]) ((ArrayImpl) value).getArray()));
          }
          return fromObjectValue(array);

        default:
          LOG.debug(
              "Unchecked sql type: {}, return Object type {}",
              sqlType,
              value.getClass().getTypeName());
          return fromObjectValue(value);
      }
    } catch (SQLException e) {
      LOG.error("Error converting SQL type {}: {}", sqlType, e.getMessage());
      throw e;
    }
  }

  // START - TODO Refactor Parse Logic
  /** Construct ExprValue from Object. */
  public static ExprValue fromObjectValue(Object o) {
    if (null == o) {
      return LITERAL_NULL;
    }
    if (o instanceof Map) {
      return tupleValue((Map) o);
    } else if (o instanceof List) {
      return collectionValue(((List) o));
    } else if (o instanceof Byte) {
      return byteValue((Byte) o);
    } else if (o instanceof Short) {
      return shortValue((Short) o);
    } else if (o instanceof Integer) {
      return integerValue((Integer) o);
    } else if (o instanceof Long) {
      return longValue(((Long) o));
    } else if (o instanceof Boolean) {
      return booleanValue((Boolean) o);
    } else if (o instanceof Double d) {
      if (Double.isNaN(d)) {
        return LITERAL_NULL;
      }
      return doubleValue(d);
    } else if (o instanceof BigDecimal d) {
      // TODO fallback decimal to double in v2
      // until https://github.com/opensearch-project/sql/issues/3619 fixed.
      return new ExprDoubleValue(d);
    } else if (o instanceof String) {
      return stringValue((String) o);
    } else if (o instanceof Float f) {
      if (Float.isNaN(f)) {
        return LITERAL_NULL;
      }
      return floatValue(f);
    } else if (o instanceof Date) {
      return dateValue(((Date) o).toLocalDate());
    } else if (o instanceof LocalDate) {
      return dateValue((LocalDate) o);
    } else if (o instanceof Time) {
      return timeValue(((Time) o).toLocalTime());
    } else if (o instanceof LocalTime) {
      return timeValue((LocalTime) o);
    } else if (o instanceof Instant) {
      return timestampValue((Instant) o);
    } else if (o instanceof Timestamp) {
      return timestampValue(((Timestamp) o).toInstant());
    } else if (o instanceof LocalDateTime) {
      return timestampValue(((LocalDateTime) o).toInstant(ZoneOffset.UTC));
    } else if (o instanceof TemporalAmount) {
      return intervalValue((TemporalAmount) o);
    } else if (o instanceof Point point) {
      return new OpenSearchExprGeoPointValue(point.getY(), point.getX());
    } else if (o instanceof OpenSearchExprGeoPointValue.GeoPoint point) {
      return new OpenSearchExprGeoPointValue(point.getLat(), point.getLon());
    } else {
      throw new ExpressionEvaluationException("unsupported object " + o.getClass());
    }
  }

  /** {@link ExprTupleValue} constructor. */
  public static ExprValue tupleValue(Map<String, Object> map) {
    LinkedHashMap<String, ExprValue> valueMap = new LinkedHashMap<>();
    map.forEach(
        (k, v) -> valueMap.put(k, v instanceof ExprValue ? (ExprValue) v : fromObjectValue(v)));
    return new ExprTupleValue(valueMap);
  }

  /** {@link ExprCollectionValue} constructor. */
  public static ExprValue collectionValue(List<Object> list) {
    List<ExprValue> valueList = new ArrayList<>();
    list.forEach(o -> valueList.add(fromObjectValue(o)));
    return new ExprCollectionValue(valueList);
  }
  // END - TODO Refactor Parse Logic
}
