/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.dqe.iceberg;

import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.Column;
import io.trino.parquet.Field;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.predicate.PredicateUtils;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergParquetColumnIOConverter;
import io.trino.plugin.iceberg.IcebergParquetColumnIOConverter.FieldContext;
import io.trino.spi.Page;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import io.trino.filesystem.Location;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.parquet.ParquetTypeUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type.Repetition;
import org.joda.time.DateTimeZone;
import org.opensearch.sql.dqe.operator.Operator;

/**
 * Reads a Parquet file and produces Trino Pages. Wraps the copied Trino ParquetReader to implement
 * the DQE {@link Operator} interface.
 */
public class ParquetPageSource implements Operator {

  private final ParquetReader parquetReader;
  private final ParquetDataSource dataSource;

  /**
   * Create a ParquetPageSource for the given file and column projection.
   *
   * @param filePath path to the Parquet file on local filesystem
   * @param icebergSchema the Iceberg table schema (for field ID-based column mapping)
   * @param requestedColumns column names to read
   * @param columnTypeMap column name to Trino type mapping
   * @param options Parquet reader options
   */
  public ParquetPageSource(
      String filePath,
      Schema icebergSchema,
      List<String> requestedColumns,
      Map<String, Type> columnTypeMap,
      ParquetReaderOptions options)
      throws IOException {
    File file = new File(filePath);
    LocalInputFile inputFile = new LocalInputFile(file);
    this.dataSource =
        new TrinoLocalParquetDataSource(
            new ParquetDataSourceId(filePath), inputFile, options);

    ParquetMetadata parquetMetadata =
        MetadataReader.readFooter(dataSource, Optional.empty());
    FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
    MessageType fileSchema = fileMetaData.getSchema();

    // Build requested Parquet schema from Iceberg field IDs
    List<org.apache.parquet.schema.Type> requestedParquetFields = new ArrayList<>();
    for (String colName : requestedColumns) {
      Types.NestedField icebergField = icebergSchema.findField(colName);
      if (icebergField == null) continue;
      int fieldId = icebergField.fieldId();
      for (org.apache.parquet.schema.Type pqField : fileSchema.getFields()) {
        org.apache.parquet.schema.Type.ID id = pqField.getId();
        if (id != null && id.intValue() == fieldId) {
          requestedParquetFields.add(pqField);
          break;
        }
      }
    }
    MessageType requestedSchema =
        new MessageType(fileSchema.getName(), requestedParquetFields);

    MessageColumnIO messageColumnIO =
        ParquetTypeUtils.getColumnIO(fileSchema, requestedSchema);

    // Build Column list using Iceberg field ID-based mapping
    List<Column> columnFields = new ArrayList<>();
    for (String colName : requestedColumns) {
      Types.NestedField icebergField = icebergSchema.findField(colName);
      if (icebergField == null) continue;
      Type trinoType = columnTypeMap.get(colName);
      if (trinoType == null) continue;
      ColumnIdentity identity = ColumnIdentity.createColumnIdentity(icebergField);
      ColumnIO columnIO = ParquetTypeUtils.lookupColumnById(messageColumnIO, icebergField.fieldId());
      Optional<Field> field =
          IcebergParquetColumnIOConverter.constructField(
              new FieldContext(trinoType, identity), columnIO);
      if (field.isPresent()) {
        columnFields.add(new Column(colName, field.get()));
      }
    }

    // Build row group infos (read all row groups — no predicate pushdown)
    List<RowGroupInfo> rowGroups = new ArrayList<>();
    long rowOffset = 0;
    for (BlockMetaData block : parquetMetadata.getBlocks()) {
      rowGroups.add(new RowGroupInfo(block, rowOffset, Optional.empty()));
      rowOffset += block.getRowCount();
    }

    AggregatedMemoryContext memoryContext = AggregatedMemoryContext.newSimpleAggregatedMemoryContext();
    this.parquetReader =
        new ParquetReader(
            Optional.ofNullable(fileMetaData.getCreatedBy()),
            columnFields,
            rowGroups,
            dataSource,
            DateTimeZone.UTC,
            memoryContext,
            options,
            e -> new RuntimeException("Parquet read error", e),
            Optional.empty(),
            Optional.empty());
  }

  /**
   * Create a ParquetPageSource with optional predicate pushdown for row-group filtering.
   */
  public ParquetPageSource(
      String filePath,
      Schema icebergSchema,
      List<String> requestedColumns,
      Map<String, Type> columnTypeMap,
      ParquetReaderOptions options,
      TupleDomain<ColumnDescriptor> parquetPredicate)
      throws IOException {
    File file = new File(filePath);
    LocalInputFile inputFile = new LocalInputFile(file);
    this.dataSource =
        new TrinoLocalParquetDataSource(
            new ParquetDataSourceId(filePath), inputFile, options);

    ParquetMetadata parquetMetadata =
        MetadataReader.readFooter(dataSource, Optional.empty());
    FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
    MessageType fileSchema = fileMetaData.getSchema();

    // Build requested Parquet schema from Iceberg field IDs
    List<org.apache.parquet.schema.Type> selectedFields = new ArrayList<>();
    for (String colName : requestedColumns) {
      Types.NestedField iceField = icebergSchema.findField(colName);
      if (iceField != null) {
        int fieldId = iceField.fieldId();
        for (org.apache.parquet.schema.Type field : fileSchema.getFields()) {
          org.apache.parquet.schema.Type.ID id = field.getId();
          if (id != null && id.intValue() == fieldId) {
            selectedFields.add(field);
            break;
          }
        }
      }
    }
    MessageType requestedSchema = new MessageType(fileSchema.getName(), selectedFields);

    MessageColumnIO messageColumnIO =
        ParquetTypeUtils.getColumnIO(fileSchema, requestedSchema);

    // Build Column list using Iceberg field ID-based mapping
    List<Column> columnFields = new ArrayList<>();
    for (String colName : requestedColumns) {
      Types.NestedField icebergField = icebergSchema.findField(colName);
      if (icebergField == null) continue;
      Type trinoType = columnTypeMap.get(colName);
      if (trinoType == null) continue;
      ColumnIdentity identity = ColumnIdentity.createColumnIdentity(icebergField);
      ColumnIO columnIO = ParquetTypeUtils.lookupColumnById(messageColumnIO, icebergField.fieldId());
      Optional<Field> field =
          IcebergParquetColumnIOConverter.constructField(
              new FieldContext(trinoType, identity), columnIO);
      if (field.isPresent()) {
        columnFields.add(new Column(colName, field.get()));
      }
    }

    // Row group filtering with predicate pushdown
    List<RowGroupInfo> rowGroups;
    Optional<TupleDomainParquetPredicate> readerPredicate = Optional.empty();

    if (parquetPredicate != null && !parquetPredicate.isAll() && !parquetPredicate.isNone()) {
      Map<List<String>, ColumnDescriptor> descriptorsByPath = new LinkedHashMap<>();
      for (ColumnDescriptor col : fileSchema.getColumns()) {
        descriptorsByPath.put(Arrays.asList(col.getPath()), col);
      }

      TupleDomainParquetPredicate tdPredicate = new TupleDomainParquetPredicate(
          parquetPredicate,
          fileSchema.getColumns(),
          DateTimeZone.UTC);
      readerPredicate = Optional.of(tdPredicate);

      rowGroups = PredicateUtils.getFilteredRowGroups(
          0L,
          Long.MAX_VALUE,
          dataSource,
          parquetMetadata.getBlocks(),
          List.of(parquetPredicate),
          List.of(tdPredicate),
          descriptorsByPath,
          DateTimeZone.UTC,
          32,
          options);
    } else {
      rowGroups = new ArrayList<>();
      long rowOffset = 0;
      for (BlockMetaData block : parquetMetadata.getBlocks()) {
        rowGroups.add(new RowGroupInfo(block, rowOffset, Optional.empty()));
        rowOffset += block.getRowCount();
      }
    }

    AggregatedMemoryContext memoryContext =
        AggregatedMemoryContext.newSimpleAggregatedMemoryContext();

    this.parquetReader =
        new ParquetReader(
            Optional.ofNullable(fileMetaData.getCreatedBy()),
            columnFields,
            rowGroups,
            dataSource,
            DateTimeZone.UTC,
            memoryContext,
            options,
            e -> new RuntimeException("Parquet read error", e),
            readerPredicate,
            Optional.empty());
  }

  @Override
  public Page processNextBatch() {
    try {
      Page page = parquetReader.nextPage();
      if (page == null) {
        return null;
      }
      return page.getLoadedPage();
    } catch (IOException e) {
      throw new RuntimeException("Error reading Parquet page", e);
    }
  }

  @Override
  public void close() {
    try {
      parquetReader.close();
    } catch (IOException ignored) {
    }
    try {
      dataSource.close();
    } catch (IOException ignored) {
    }
  }

  /**
   * Simple ParquetDataSource backed by a local TrinoInputFile. Extends AbstractParquetDataSource
   * to provide the readFully/readTail implementations via TrinoInput.
   */
  public static class TrinoLocalParquetDataSource
      extends io.trino.parquet.AbstractParquetDataSource {

    private final LocalInputFile inputFile;

    public TrinoLocalParquetDataSource(
        ParquetDataSourceId id, LocalInputFile inputFile, ParquetReaderOptions options)
        throws IOException {
      super(id, inputFile.length(), options);
      this.inputFile = inputFile;
    }

    @Override
    protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
        throws IOException {
      try (io.trino.filesystem.TrinoInput input = inputFile.newInput()) {
        input.readFully(position, buffer, bufferOffset, bufferLength);
      }
    }
  }
}
