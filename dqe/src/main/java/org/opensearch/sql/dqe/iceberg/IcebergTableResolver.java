/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.iceberg;

import io.trino.plugin.iceberg.TypeConverter;
import io.trino.spi.type.Type;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo;
import org.opensearch.sql.dqe.coordinator.metadata.TableInfo.ColumnInfo;

/**
 * Resolves Iceberg table metadata from a naming convention {@code iceberg.db.table}. Loads the
 * table directly from metadata JSON files on the local filesystem without Hadoop.
 */
public class IcebergTableResolver {

  private static final String ICEBERG_PREFIX = "iceberg.";
  private final String warehousePath;
  private final FileIO fileIO;

  public IcebergTableResolver(String warehousePath) {
    this.warehousePath = warehousePath;
    this.fileIO = new LocalFileIO();
  }

  /** Returns true if the table name follows the {@code iceberg.db.table} convention. */
  public static boolean isIcebergTable(String tableName) {
    return tableName != null
        && tableName.startsWith(ICEBERG_PREFIX)
        && tableName.indexOf('.', ICEBERG_PREFIX.length()) > 0;
  }

  /** Load the Iceberg table and convert its schema to a {@link TableInfo}. */
  public TableInfo resolve(String tableName) {
    Table icebergTable = loadTable(tableName);
    Schema schema = icebergTable.schema();

    List<ColumnInfo> columns = new ArrayList<>();
    for (Types.NestedField field : schema.columns()) {
      Type trinoType = TypeConverter.toTrinoType(field.type(), null);
      String osType = trinoTypeToOsType(trinoType);
      columns.add(new ColumnInfo(field.name(), osType, trinoType));
    }
    return new TableInfo(tableName, columns);
  }

  /** Load the raw Iceberg Table object for scan planning. */
  public Table loadTable(String tableName) {
    String suffix = tableName.substring(ICEBERG_PREFIX.length());
    int dot = suffix.indexOf('.');
    String db = suffix.substring(0, dot);
    String table = suffix.substring(dot + 1);
    String tablePath = warehousePath + "/" + db + "/" + table;

    String metadataLocation = findLatestMetadata(tablePath);
    TableMetadata metadata = TableMetadataParser.read(fileIO, metadataLocation);
    StaticTableOperations ops = new StaticTableOperations(metadata, fileIO);
    return new BaseTable(ops, table);
  }

  /**
   * Find the latest metadata JSON file in the table's metadata directory. Checks
   * version-hint.text first, then falls back to the highest-numbered v*.metadata.json file.
   */
  private static String findLatestMetadata(String tablePath) {
    File metadataDir = new File(tablePath, "metadata");
    if (!metadataDir.isDirectory()) {
      throw new RuntimeException("Metadata directory not found: " + metadataDir);
    }

    // Try version-hint.text
    File versionHint = new File(metadataDir, "version-hint.text");
    if (versionHint.exists()) {
      try {
        String version = new String(java.nio.file.Files.readAllBytes(versionHint.toPath())).trim();
        File vFile = new File(metadataDir, "v" + version + ".metadata.json");
        if (vFile.exists()) {
          return vFile.getAbsolutePath();
        }
      } catch (Exception e) {
        // Fall through to directory scan
      }
    }

    // Fall back: pick highest-numbered *.metadata.json
    File[] metadataFiles =
        metadataDir.listFiles((dir, name) -> name.endsWith(".metadata.json"));
    if (metadataFiles == null || metadataFiles.length == 0) {
      throw new RuntimeException("No metadata JSON files found in: " + metadataDir);
    }
    Arrays.sort(metadataFiles);
    return metadataFiles[metadataFiles.length - 1].getAbsolutePath();
  }

  private static String trinoTypeToOsType(Type type) {
    if (type instanceof io.trino.spi.type.BigintType) return "long";
    if (type instanceof io.trino.spi.type.IntegerType) return "integer";
    if (type instanceof io.trino.spi.type.DoubleType) return "double";
    if (type instanceof io.trino.spi.type.RealType) return "float";
    if (type instanceof io.trino.spi.type.BooleanType) return "boolean";
    if (type instanceof io.trino.spi.type.VarcharType) return "keyword";
    if (type instanceof io.trino.spi.type.TimestampType) return "date";
    if (type instanceof io.trino.spi.type.TimestampWithTimeZoneType) return "date";
    if (type instanceof io.trino.spi.type.DateType) return "date";
    return "keyword";
  }
}
