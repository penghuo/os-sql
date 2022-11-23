/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.filesystem.storage.fs;

import static org.opensearch.sql.data.type.ExprCoreType.UNKNOWN;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterators;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.ToString;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.common.utils.AccessController;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.filesystem.storage.split.FileSystemSplit;
import org.opensearch.sql.storage.TableScanOperator;
import org.opensearch.sql.storage.split.Split;

@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class FSScanOperator extends TableScanOperator {

  private static final Logger log = LogManager.getLogger(FSScanOperator.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final FileSystem fs;

  private final Path basePath;

  private final FSExprValueFactory exprValueFactory;

  private final CompressionCodec compressionCodec;

  private Iterator<FSFileReader> fileReaderIterator = null;

  private Optional<FSFileReader> currentFileReader = Optional.empty();

  public FSScanOperator(
      FileSystem fs,
      Path basePath,
      CompressionCodec compressionCodec,
      Map<String, ExprType> fieldTypes) {
    this.fs = fs;
    this.basePath = basePath;
    this.exprValueFactory = new FSExprValueFactory(fieldTypes);
    this.compressionCodec = compressionCodec;
  }

  @SneakyThrows
  @Override
  public void add(Split split) {
    fileReaderIterator =
        Iterators.transform(((FileSystemSplit) split).getPaths().iterator(), FSFileReader::new);
  }

  @Override
  public void open() {
    if (fileReaderIterator == null) {
      FileStatus[] fileStatuses = AccessController.doPrivileged(() -> fs.listStatus(basePath));
      Set<Path> allFiles =
          Arrays.stream(fileStatuses)
              .filter(status -> !status.isDirectory())
              .map(FileStatus::getPath)
              .collect(Collectors.toSet());
      fileReaderIterator = Iterators.transform(allFiles.iterator(), FSFileReader::new);
    }
  }

  @Override
  public boolean hasNext() {
    if (currentFileReader.isEmpty() || !currentFileReader.get().hasNext()) {
      currentFileReader.ifPresent(FSFileReader::close);
      currentFileReader =
          fileReaderIterator.hasNext() ? Optional.of(fileReaderIterator.next()) : Optional.empty();
    }
    return currentFileReader.map(FSFileReader::hasNext).orElse(false);
  }

  @Override
  public ExprValue next() {
    return currentFileReader.get().next();
  }

  /**
   * close current file reader.
   */
  @Override
  public void close() {
    currentFileReader.ifPresent(FSFileReader::close);
  }

  @Override
  public String explain() {
    return StringUtils.format("Filesystem:%s Scan Operator", fs.getScheme());
  }

  /**
   * Expr value factory for S3 data.
   */
  @RequiredArgsConstructor
  public static class FSExprValueFactory {
    private final Map<String, ExprType> fieldTypes;

    public ExprValue construct(Map<String, Object> data) {
      LinkedHashMap<String, ExprValue> valueMap = new LinkedHashMap<>();
      data.forEach((k, v) -> valueMap.put(k, construct(k, v)));
      return new ExprTupleValue(valueMap);
    }

    private ExprValue construct(String fieldName, Object value) {
      ExprType type = fieldTypes.getOrDefault(fieldName, UNKNOWN);
      if (type == UNKNOWN) {
        return ExprValueUtils.nullValue();
      }
      return ExprValueUtils.fromObjectValue(value, (ExprCoreType) type);
    }
  }

  /**
   * Single File Reader.
   */
  @RequiredArgsConstructor
  class FSFileReader implements Iterator<ExprValue>, AutoCloseable {

    private final BufferedReader reader;

    private final Path path;

    @SneakyThrows
    public FSFileReader(Path path) {
      this.path = path;

      FSDataInputStream stream = AccessController.doPrivileged(() -> fs.open(path));
      CompressionInputStream compressionInputStream =
          AccessController.doPrivileged(() -> compressionCodec.createInputStream(stream));
      reader =
          AccessController.doPrivileged(
              () -> new BufferedReader(new InputStreamReader(compressionInputStream)));
      log.debug("read file {}", path);
    }

    @SneakyThrows
    public boolean hasNext() {
      return reader.ready();
    }

    @SneakyThrows
    public ExprValue next() {
      TypeReference<Map<String, Object>> typeRef = new TypeReference<>() {};
      return exprValueFactory.construct(OBJECT_MAPPER.readValue(reader.readLine(), typeRef));
    }

    @SneakyThrows
    public void close() {
      log.debug("close file {}", path);
      reader.close();
    }
  }
}
