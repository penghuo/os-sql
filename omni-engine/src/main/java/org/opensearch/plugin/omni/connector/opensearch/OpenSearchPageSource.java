/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.engine.Engine;
import org.opensearch.plugin.omni.connector.opensearch.decoder.FieldDecoder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Queue;

import static java.util.Objects.requireNonNull;

/**
 * Reads data from OpenSearch indices using Lucene's BulkScorer + Collector pattern.
 *
 * Uses IndexSearcher.search(query, collector) which internally uses BulkScorer
 * for batched iteration (2048 docs per batch). This matches how OpenSearch's own
 * aggregation framework reads data and is 10-100x faster than per-doc Scorer iteration.
 *
 * For primitive columns (keyword, numeric, date, boolean), reads Lucene doc_values
 * directly — no _source JSON parsing. Falls back to _source only for text/nested/object.
 */
public class OpenSearchPageSource
        implements ConnectorPageSource
{
    private static final Logger logger = LogManager.getLogger(OpenSearchPageSource.class);
    private static final int BATCH_SIZE = 8192;

    private final Engine.Searcher searcher;
    private final List<OpenSearchColumnHandle> columns;
    private final List<FieldDecoder> decoders;
    private final IndexSearcher indexSearcher;
    private final Query query;
    private final OptionalLong limit;

    // Pre-collected pages from BulkScorer (per-segment incremental delivery)
    private final Queue<Page> pageQueue = new ArrayDeque<>();

    // Segment targeting
    private final LeafReaderContext[] leaves;
    private int currentLeafIndex;

    // Tracking
    private long completedPositions;
    private long completedBytes;
    private boolean finished;
    private boolean closed;

    // For doc-range targeting
    private final int targetMinDoc;
    private final int targetMaxDoc;

    public OpenSearchPageSource(
            Engine.Searcher searcher,
            List<OpenSearchColumnHandle> columns,
            List<FieldDecoder> decoders,
            Query query,
            OpenSearchSplit split,
            OptionalLong limit)
    {
        this.searcher = requireNonNull(searcher);
        this.columns = requireNonNull(columns);
        this.decoders = requireNonNull(decoders);
        this.query = requireNonNull(query);
        this.limit = limit;
        this.indexSearcher = new IndexSearcher(searcher.getDirectoryReader());

        this.targetMinDoc = split.getMinDoc();
        this.targetMaxDoc = split.getMaxDoc();

        if (split.hasSegmentTarget()) {
            List<LeafReaderContext> allLeaves = indexSearcher.getIndexReader().leaves();
            LeafReaderContext matched = null;
            for (LeafReaderContext leaf : allLeaves) {
                org.apache.lucene.index.LeafReader unwrapped =
                        org.apache.lucene.index.FilterLeafReader.unwrap(leaf.reader());
                if (unwrapped instanceof org.apache.lucene.index.SegmentReader segReader) {
                    String leafSegName = segReader.getSegmentInfo().info.name;
                    if (leafSegName.equals(split.getSegmentName())) {
                        matched = leaf;
                        break;
                    }
                }
            }
            if (matched != null) {
                this.leaves = new LeafReaderContext[]{matched};
            } else {
                this.leaves = new LeafReaderContext[0];
                this.finished = true;
            }
        } else {
            this.leaves = indexSearcher.getIndexReader().leaves().toArray(new LeafReaderContext[0]);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return finished || closed;
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public Page getNextPage()
    {
        if (finished || closed) {
            return null;
        }

        // Dequeue buffered pages first; if empty, process next segment
        while (pageQueue.isEmpty() && !finished) {
            if (currentLeafIndex >= leaves.length) {
                finished = true;
                return null;
            }
            try {
                collectSegmentPages(leaves[currentLeafIndex]);
            } catch (IOException e) {
                throw new UncheckedIOException("Error scanning OpenSearch index", e);
            }
            currentLeafIndex++;
        }

        Page page = pageQueue.poll();
        if (page == null) {
            finished = true;
        }
        return page;
    }

    /**
     * Process ONE segment using BulkScorer + Collector pattern.
     * Pages are buffered in pageQueue and dequeued across multiple getNextPage() calls.
     * BulkScorer processes docs in batches of 2048 internally, enabling JIT inlining
     * and CPU cache locality — 10-100x faster than per-doc Scorer iteration.
     */
    private void collectSegmentPages(LeafReaderContext leaf) throws IOException
    {
        Weight weight = indexSearcher.createWeight(
                indexSearcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);

        if (finished) return;

        // Get BulkScorer for this leaf
        BulkScorer bulkScorer = weight.bulkScorer(leaf);
        if (bulkScorer == null) {
            return; // No matching docs in this segment
        }

        // Create doc_values readers for this leaf
        LeafReader leafReader = leaf.reader();
        DocValuesReader[] leafDvReaders = new DocValuesReader[columns.size()];
        boolean leafNeedsSource = false;
        for (int i = 0; i < columns.size(); i++) {
            String trinoName = columns.get(i).getName();
            String osName = columns.get(i).getOpensearchName();
            io.trino.spi.type.Type type = columns.get(i).getType();
            if (trinoName.startsWith("_")) {
                leafNeedsSource = true;
                continue;
            }
            if (DocValuesReader.isSupported(type)) {
                // Use original OpenSearch field name for doc_values lookup
                leafDvReaders[i] = DocValuesReader.create(type, leafReader, osName);
                if (leafDvReaders[i] == null) {
                    leafNeedsSource = true;
                }
            } else {
                leafNeedsSource = true;
            }
        }

        StoredFields storedFields = leafNeedsSource ? leafReader.storedFields() : null;
        final boolean needsSourceForLeaf = leafNeedsSource;

        // Check which columns support batch reading (DictionaryBlock)
        final boolean[] batchColumns = new boolean[columns.size()];
        boolean anyBatchColumn = false;
        for (int i = 0; i < columns.size(); i++) {
            if (leafDvReaders[i] != null && leafDvReaders[i].supportsBatchRead()) {
                batchColumns[i] = true;
                anyBatchColumn = true;
            }
        }

        // Mutable state for the collector
        BlockBuilder[] builders = new BlockBuilder[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            if (!batchColumns[i]) {
                builders[i] = columns.get(i).getType().createBlockBuilder(null, BATCH_SIZE);
            }
        }
        final int[] rowCount = {0};
        // Collect doc IDs for batch columns
        final int[] batchDocIds = anyBatchColumn ? new int[BATCH_SIZE] : null;

        // Create LeafCollector — called by BulkScorer in tight inner loop
        LeafCollector collector = new LeafCollector() {
            @Override
            public void setScorer(Scorable scorer) {}

            @Override
            public void collect(int doc) throws IOException
            {
                // Check limit
                if (limit.isPresent() && completedPositions >= limit.getAsLong()) {
                    throw new CollectionTerminatedException();
                }

                // Doc range filtering
                if (targetMinDoc >= 0 && doc < targetMinDoc) return;
                if (targetMaxDoc >= 0 && doc >= targetMaxDoc) {
                    throw new CollectionTerminatedException();
                }

                // Save doc ID for batch columns
                if (batchDocIds != null) {
                    batchDocIds[rowCount[0]] = doc;
                }

                // Read _source only if needed
                Map<String, Object> source = null;
                Document storedDoc = null;
                if (needsSourceForLeaf) {
                    storedDoc = storedFields.document(doc);
                    BytesRef sourceRef = storedDoc.getBinaryValue("_source");
                    if (sourceRef != null) {
                        byte[] sourceBytes = new byte[sourceRef.length];
                        System.arraycopy(sourceRef.bytes, sourceRef.offset,
                                sourceBytes, 0, sourceRef.length);
                        source = XContentHelper.convertToMap(
                                BytesReference.fromByteBuffer(ByteBuffer.wrap(sourceBytes)),
                                true, XContentType.JSON).v2();
                        completedBytes += sourceRef.length;
                    }
                }

                // Decode non-batch columns per row (numerics via BlockBuilder, _source fallback)
                for (int i = 0; i < columns.size(); i++) {
                    if (batchColumns[i]) {
                        continue; // Skip — will be handled in flushPage via readBatch
                    }
                    if (leafDvReaders[i] != null) {
                        leafDvReaders[i].read(doc, builders[i]);
                    } else if (source != null) {
                        String trinoName = columns.get(i).getName();
                        String osName = columns.get(i).getOpensearchName();
                        Object value;
                        if ("_id".equals(trinoName)) {
                            value = storedDoc != null ? storedDoc.get("_id") : null;
                        } else if ("_source".equals(trinoName)) {
                            value = source;
                        } else if ("_score".equals(trinoName)) {
                            value = 0.0f;
                        } else {
                            value = extractField(source, osName);
                        }
                        decoders.get(i).decode(value, builders[i]);
                    } else {
                        builders[i].appendNull();
                    }
                }

                completedPositions++;
                rowCount[0]++;

                // Flush page when batch is full
                if (rowCount[0] >= BATCH_SIZE) {
                    flushPageWithBatch(builders, batchColumns, leafDvReaders, batchDocIds, rowCount[0]);
                    rowCount[0] = 0;
                    // Re-create builders for non-batch columns only
                    for (int i = 0; i < columns.size(); i++) {
                        if (!batchColumns[i]) {
                            builders[i] = columns.get(i).getType().createBlockBuilder(null, BATCH_SIZE);
                        }
                    }
                }
            }
        };

        // Run BulkScorer — this is where the magic happens
        // BulkScorer processes docs in batches of 2048 internally
        try {
            Bits liveDocs = leafReader.getLiveDocs();
            int minDoc = (targetMinDoc >= 0) ? targetMinDoc : 0;
            int maxDoc = (targetMaxDoc >= 0) ? targetMaxDoc : leafReader.maxDoc();
            bulkScorer.score(collector, liveDocs, minDoc, maxDoc);
        } catch (CollectionTerminatedException e) {
            // Normal: limit reached or doc range exhausted
            finished = true;
        }

        // Flush remaining rows for this leaf
        if (rowCount[0] > 0) {
            flushPageWithBatch(builders, batchColumns, leafDvReaders, batchDocIds, rowCount[0]);
        }
    }

    /**
     * Flush a page, using batch readBatch() for DictionaryBlock columns
     * and BlockBuilder.build() for other columns.
     */
    private void flushPageWithBatch(BlockBuilder[] builders, boolean[] batchColumns,
            DocValuesReader[] leafDvReaders, int[] batchDocIds, int rows)
    {
        try {
            Block[] blocks = new Block[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                if (batchColumns[i] && leafDvReaders[i] != null && batchDocIds != null) {
                    // Batch path: returns DictionaryBlock (ordinals only, no string copies)
                    blocks[i] = leafDvReaders[i].readBatch(batchDocIds, rows, columns.get(i).getType());
                } else {
                    blocks[i] = builders[i].build();
                }
            }
            pageQueue.add(new Page(rows, blocks));
        } catch (IOException e) {
            throw new java.io.UncheckedIOException("Error building batch page", e);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        if (!closed) {
            closed = true;
            finished = true;
            searcher.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static Object extractField(Map<String, Object> source, String fieldName)
    {
        Object value = source.get(fieldName);
        if (value != null || source.containsKey(fieldName)) {
            return value;
        }
        if (fieldName.contains(".")) {
            String[] parts = fieldName.split("\\.");
            Object current = source;
            for (String part : parts) {
                if (!(current instanceof Map)) {
                    return null;
                }
                current = ((Map<String, Object>) current).get(part);
                if (current == null) {
                    return null;
                }
            }
            return current;
        }
        return null;
    }
}
