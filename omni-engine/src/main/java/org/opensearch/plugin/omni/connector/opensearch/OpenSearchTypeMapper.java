/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.*;
import org.opensearch.plugin.omni.connector.opensearch.decoder.*;

import java.util.*;

public final class OpenSearchTypeMapper
{
    // Built-in hidden column names
    public static final String ID_COLUMN = "_id";
    public static final String SOURCE_COLUMN = "_source";
    public static final String SCORE_COLUMN = "_score";

    private OpenSearchTypeMapper() {}

    /**
     * Convert OpenSearch index mapping properties to column handles.
     *
     * @param properties the "properties" map from index mapping
     * @param meta the "_meta" map from index mapping (for array/rawJson flags), may be null
     * @return list of column handles
     */
    public static List<OpenSearchColumnHandle> mapColumns(Map<String, Object> properties, Map<String, Object> meta)
    {
        if (properties == null) {
            return ImmutableList.of();
        }
        Map<String, Object> trinoMeta = extractTrinoMeta(meta);
        List<OpenSearchColumnHandle> columns = new ArrayList<>();
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            String fieldName = entry.getKey();
            @SuppressWarnings("unchecked")
            Map<String, Object> fieldMapping = (Map<String, Object>) entry.getValue();
            OpenSearchColumnHandle column = mapField(fieldName, fieldMapping, trinoMeta);
            if (column != null) {
                columns.add(column);
            }
        }
        return columns;
    }

    /**
     * Create decoders matching the column list. Order corresponds to columns.
     */
    public static List<FieldDecoder> createDecoders(List<OpenSearchColumnHandle> columns, Map<String, Object> meta)
    {
        Map<String, Object> trinoMeta = extractTrinoMeta(meta);
        List<FieldDecoder> decoders = new ArrayList<>();
        for (OpenSearchColumnHandle column : columns) {
            decoders.add(createDecoder(column, trinoMeta));
        }
        return decoders;
    }

    @SuppressWarnings("unchecked")
    private static OpenSearchColumnHandle mapField(String name, Map<String, Object> mapping, Map<String, Object> trinoMeta)
    {
        String trinoName = name.toLowerCase(java.util.Locale.ROOT);

        // Check asRawJson flag
        if (isRawJson(name, trinoMeta)) {
            boolean isArray = isArray(name, trinoMeta);
            Type type = isArray ? new ArrayType(VarcharType.VARCHAR) : VarcharType.VARCHAR;
            return new OpenSearchColumnHandle(trinoName, name, type, false);
        }

        // Nested object
        if (mapping.containsKey("properties")) {
            // Re-wrap in HashMap to match legacy OpenSearchDescribeIndexRequest.getFieldTypes()
            // iteration order. See OpenSearchMetadata.getColumnsForSingleIndex for rationale.
            Map<String, Object> subProperties = new HashMap<>(
                    (Map<String, Object>) mapping.get("properties"));
            List<RowType.Field> rowFields = new ArrayList<>();
            for (Map.Entry<String, Object> entry : subProperties.entrySet()) {
                Map<String, Object> subMapping = (Map<String, Object>) entry.getValue();
                OpenSearchColumnHandle subCol = mapField(entry.getKey(), subMapping, trinoMeta);
                if (subCol != null) {
                    rowFields.add(new RowType.Field(Optional.of(subCol.getName()), subCol.getType()));
                }
            }
            if (rowFields.isEmpty()) return null;
            Type rowType = RowType.from(rowFields);
            boolean isArray = isArray(name, trinoMeta);
            Type type = isArray ? new ArrayType(rowType) : rowType;
            return new OpenSearchColumnHandle(trinoName, name, type, false);
        }

        String fieldType = (String) mapping.get("type");
        if (fieldType == null) return null;

        TypeAndPredicate tap = mapPrimitiveType(fieldType, mapping);
        if (tap == null) return null;

        boolean isArray = isArray(name, trinoMeta);
        Type type = isArray ? new ArrayType(tap.type) : tap.type;
        boolean supportsPredicate = isArray ? false : tap.supportsPredicate;
        return new OpenSearchColumnHandle(trinoName, name, type, supportsPredicate);
    }

    private static TypeAndPredicate mapPrimitiveType(String fieldType, Map<String, Object> mapping)
    {
        return switch (fieldType) {
            case "keyword" -> new TypeAndPredicate(VarcharType.VARCHAR, true);
            case "text" -> new TypeAndPredicate(VarcharType.VARCHAR, false);
            case "boolean" -> new TypeAndPredicate(BooleanType.BOOLEAN, true);
            case "byte" -> new TypeAndPredicate(TinyintType.TINYINT, true);
            case "short" -> new TypeAndPredicate(SmallintType.SMALLINT, true);
            case "integer" -> new TypeAndPredicate(IntegerType.INTEGER, true);
            case "long" -> new TypeAndPredicate(BigintType.BIGINT, true);
            case "float" -> new TypeAndPredicate(RealType.REAL, true);
            case "double" -> new TypeAndPredicate(DoubleType.DOUBLE, true);
            case "scaled_float" -> new TypeAndPredicate(DoubleType.DOUBLE, true);
            case "date" -> {
                // OpenSearch stores all date fields as epoch_millis in doc_values,
                // regardless of the display format. Accept all date formats.
                yield new TypeAndPredicate(TimestampType.TIMESTAMP_MILLIS, true);
            }
            case "ip" -> new TypeAndPredicate(VarcharType.VARCHAR, false);
            case "binary" -> new TypeAndPredicate(VarbinaryType.VARBINARY, false);
            default -> null; // Unknown types are skipped
        };
    }

    static FieldDecoder createDecoder(OpenSearchColumnHandle column, Map<String, Object> trinoMeta)
    {
        // Handle built-in columns
        if (ID_COLUMN.equals(column.getName())) return VarcharFieldDecoder.INSTANCE;
        if (SOURCE_COLUMN.equals(column.getName())) return RawJsonFieldDecoder.INSTANCE;
        if (SCORE_COLUMN.equals(column.getName())) return RealFieldDecoder.INSTANCE;

        // Handle arrays
        Type type = column.getType();
        if (type instanceof ArrayType arrayType) {
            FieldDecoder elementDecoder = decoderForType(arrayType.getElementType());
            return new ArrayFieldDecoder(elementDecoder);
        }

        // Handle raw json
        if (isRawJson(column.getName(), trinoMeta)) return RawJsonFieldDecoder.INSTANCE;

        return decoderForType(type);
    }

    private static FieldDecoder decoderForType(Type type)
    {
        if (type.equals(BooleanType.BOOLEAN)) return BooleanFieldDecoder.INSTANCE;
        if (type.equals(TinyintType.TINYINT)) return TinyintFieldDecoder.INSTANCE;
        if (type.equals(SmallintType.SMALLINT)) return SmallintFieldDecoder.INSTANCE;
        if (type.equals(IntegerType.INTEGER)) return IntegerFieldDecoder.INSTANCE;
        if (type.equals(BigintType.BIGINT)) return BigintFieldDecoder.INSTANCE;
        if (type.equals(RealType.REAL)) return RealFieldDecoder.INSTANCE;
        if (type.equals(DoubleType.DOUBLE)) return DoubleFieldDecoder.INSTANCE;
        if (type.equals(VarcharType.VARCHAR)) return VarcharFieldDecoder.INSTANCE;
        if (type.equals(VarbinaryType.VARBINARY)) return VarbinaryFieldDecoder.INSTANCE;
        if (type.equals(TimestampType.TIMESTAMP_MILLIS)) return TimestampFieldDecoder.INSTANCE;
        if (type instanceof RowType rowType) {
            List<String> names = new ArrayList<>();
            List<FieldDecoder> decoders = new ArrayList<>();
            for (RowType.Field field : rowType.getFields()) {
                names.add(field.getName().orElseThrow());
                decoders.add(decoderForType(field.getType()));
            }
            return new RowFieldDecoder(names, decoders);
        }
        // Fallback
        return VarcharFieldDecoder.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> extractTrinoMeta(Map<String, Object> meta)
    {
        if (meta == null) return Map.of();
        Object trino = meta.get("trino");
        if (trino instanceof Map) return (Map<String, Object>) trino;
        return Map.of();
    }

    @SuppressWarnings("unchecked")
    private static boolean isArray(String fieldName, Map<String, Object> trinoMeta)
    {
        Object fieldMeta = trinoMeta.get(fieldName);
        if (fieldMeta instanceof Map) {
            return Boolean.TRUE.equals(((Map<String, Object>) fieldMeta).get("isArray"));
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static boolean isRawJson(String fieldName, Map<String, Object> trinoMeta)
    {
        Object fieldMeta = trinoMeta.get(fieldName);
        if (fieldMeta instanceof Map) {
            return Boolean.TRUE.equals(((Map<String, Object>) fieldMeta).get("asRawJson"));
        }
        return false;
    }

    private record TypeAndPredicate(Type type, boolean supportsPredicate) {}
}
