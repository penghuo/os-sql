/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.rest;

import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Static utility methods for serializing query results to JSON.
 * TODO Phase 3: add methods that take actual Trino Page/Block objects.
 */
public final class ResultSerializer {

    private ResultSerializer() {}

    /** Writes rows as a JSON array of arrays. */
    public static XContentBuilder serializeRows(XContentBuilder builder, List<List<Object>> rows) throws IOException {
        builder.startArray("rows");
        for (List<Object> row : rows) {
            builder.startArray();
            for (Object value : row) {
                builder.value(value);
            }
            builder.endArray();
        }
        builder.endArray();
        return builder;
    }

    /** Writes column metadata as a JSON array of objects with name and type. */
    public static XContentBuilder serializeColumns(XContentBuilder builder, List<String> columnNames, List<String> columnTypes)
            throws IOException {
        builder.startArray("columns");
        for (int i = 0; i < columnNames.size(); i++) {
            builder.startObject();
            builder.field("name", columnNames.get(i));
            builder.field("type", columnTypes.get(i));
            builder.endObject();
        }
        builder.endArray();
        return builder;
    }
}
