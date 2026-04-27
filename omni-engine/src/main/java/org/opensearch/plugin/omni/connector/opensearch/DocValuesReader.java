/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.omni.connector.opensearch;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.block.VariableWidthBlock;
import io.trino.spi.type.*;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Optional;

/**
 * Reads field values directly from Lucene doc_values (columnar format).
 * Much faster than parsing _source JSON — no decompression, no JSON parsing.
 * Used for primitive fields: keyword, long, integer, double, float, date, boolean, ip.
 */
public abstract class DocValuesReader
{
    /** Read the value for docId and write to the BlockBuilder. */
    public abstract void read(int docId, BlockBuilder output) throws IOException;

    /**
     * Read a batch of values and return a Block directly (bypasses BlockBuilder).
     * Default implementation falls back to per-row BlockBuilder path.
     * Subclasses (e.g., keyword DictionaryBlock reader) override for batch efficiency.
     */
    public Block readBatch(int[] docIds, int count, Type type) throws IOException
    {
        // Default: use BlockBuilder (per-row, slower)
        BlockBuilder builder = type.createBlockBuilder(null, count);
        for (int i = 0; i < count; i++) {
            read(docIds[i], builder);
        }
        return builder.build();
    }

    /** Whether this reader supports batch reading (returns true if readBatch is optimized). */
    public boolean supportsBatchRead()
    {
        return false;
    }

    /**
     * Get a numeric doc_values reader. OpenSearch stores most numeric fields as
     * SortedNumericDocValues (not NumericDocValues), so we try both.
     */
    private static SortedNumericDocValues getNumericDV(LeafReader reader, String field) throws IOException
    {
        // Try SortedNumericDocValues first (OpenSearch's default for all numeric types)
        SortedNumericDocValues sndv = reader.getSortedNumericDocValues(field);
        if (sndv != null) return sndv;

        // Fallback: wrap single-valued NumericDocValues as SortedNumeric
        NumericDocValues ndv = reader.getNumericDocValues(field);
        if (ndv != null) {
            return new SortedNumericDocValues() {
                private boolean exists;
                @Override public boolean advanceExact(int target) throws IOException { exists = ndv.advanceExact(target); return exists; }
                @Override public int docValueCount() { return exists ? 1 : 0; }
                @Override public long nextValue() throws IOException { return ndv.longValue(); }
                @Override public int docID() { return ndv.docID(); }
                @Override public int nextDoc() throws IOException { return ndv.nextDoc(); }
                @Override public int advance(int target) throws IOException { return ndv.advance(target); }
                @Override public long cost() { return ndv.cost(); }
            };
        }
        return null;
    }

    /** Batch reader for BIGINT, DOUBLE, TIMESTAMP → LongArrayBlock */
    private static DocValuesReader forLongBlock(LeafReader reader, String field, Type type, boolean scaleMillisToMicros) throws IOException
    {
        SortedNumericDocValues dv = getNumericDV(reader, field);
        if (dv == null) return null;
        return new DocValuesReader() {
            @Override
            public void read(int docId, BlockBuilder output) throws IOException
            {
                if (dv.advanceExact(docId) && dv.docValueCount() > 0) {
                    long v = dv.nextValue();
                    type.writeLong(output, scaleMillisToMicros ? v * 1000 : v);
                } else {
                    output.appendNull();
                }
            }

            @Override
            public Block readBatch(int[] docIds, int count, Type type) throws IOException
            {
                long[] values = new long[count];
                boolean[] nulls = new boolean[count];
                boolean hasNull = false;
                for (int i = 0; i < count; i++) {
                    if (dv.advanceExact(docIds[i]) && dv.docValueCount() > 0) {
                        long v = dv.nextValue();
                        values[i] = scaleMillisToMicros ? v * 1000 : v;
                    } else {
                        nulls[i] = true;
                        hasNull = true;
                    }
                }
                return new LongArrayBlock(count, hasNull ? Optional.of(nulls) : Optional.empty(), values);
            }

            @Override
            public boolean supportsBatchRead() { return true; }
        };
    }

    /** Batch reader for INTEGER, REAL → IntArrayBlock */
    private static DocValuesReader forIntBlock(LeafReader reader, String field, Type type) throws IOException
    {
        SortedNumericDocValues dv = getNumericDV(reader, field);
        if (dv == null) return null;
        return new DocValuesReader() {
            @Override
            public void read(int docId, BlockBuilder output) throws IOException
            {
                if (dv.advanceExact(docId) && dv.docValueCount() > 0) {
                    type.writeLong(output, (int) dv.nextValue());
                } else {
                    output.appendNull();
                }
            }

            @Override
            public Block readBatch(int[] docIds, int count, Type type) throws IOException
            {
                int[] values = new int[count];
                boolean[] nulls = new boolean[count];
                boolean hasNull = false;
                for (int i = 0; i < count; i++) {
                    if (dv.advanceExact(docIds[i]) && dv.docValueCount() > 0) {
                        values[i] = (int) dv.nextValue();
                    } else {
                        nulls[i] = true;
                        hasNull = true;
                    }
                }
                return new IntArrayBlock(count, hasNull ? Optional.of(nulls) : Optional.empty(), values);
            }

            @Override
            public boolean supportsBatchRead() { return true; }
        };
    }

    /** Batch reader for SMALLINT → ShortArrayBlock */
    private static DocValuesReader forShortBlock(LeafReader reader, String field) throws IOException
    {
        SortedNumericDocValues dv = getNumericDV(reader, field);
        if (dv == null) return null;
        return new DocValuesReader() {
            @Override
            public void read(int docId, BlockBuilder output) throws IOException
            {
                if (dv.advanceExact(docId) && dv.docValueCount() > 0) {
                    SmallintType.SMALLINT.writeLong(output, (short) dv.nextValue());
                } else {
                    output.appendNull();
                }
            }

            @Override
            public Block readBatch(int[] docIds, int count, Type type) throws IOException
            {
                short[] values = new short[count];
                boolean[] nulls = new boolean[count];
                boolean hasNull = false;
                for (int i = 0; i < count; i++) {
                    if (dv.advanceExact(docIds[i]) && dv.docValueCount() > 0) {
                        values[i] = (short) dv.nextValue();
                    } else {
                        nulls[i] = true;
                        hasNull = true;
                    }
                }
                return new ShortArrayBlock(count, hasNull ? Optional.of(nulls) : Optional.empty(), values);
            }

            @Override
            public boolean supportsBatchRead() { return true; }
        };
    }

    /** Batch reader for TINYINT, BOOLEAN → ByteArrayBlock */
    private static DocValuesReader forByteBlock(LeafReader reader, String field, Type type, boolean isBoolean) throws IOException
    {
        SortedNumericDocValues dv = getNumericDV(reader, field);
        if (dv == null) return null;
        return new DocValuesReader() {
            @Override
            public void read(int docId, BlockBuilder output) throws IOException
            {
                if (dv.advanceExact(docId) && dv.docValueCount() > 0) {
                    long v = dv.nextValue();
                    if (isBoolean) {
                        BooleanType.BOOLEAN.writeBoolean(output, v != 0);
                    } else {
                        type.writeLong(output, (byte) v);
                    }
                } else {
                    output.appendNull();
                }
            }

            @Override
            public Block readBatch(int[] docIds, int count, Type type) throws IOException
            {
                byte[] values = new byte[count];
                boolean[] nulls = new boolean[count];
                boolean hasNull = false;
                for (int i = 0; i < count; i++) {
                    if (dv.advanceExact(docIds[i]) && dv.docValueCount() > 0) {
                        long v = dv.nextValue();
                        values[i] = isBoolean ? (byte) (v != 0 ? 1 : 0) : (byte) v;
                    } else {
                        nulls[i] = true;
                        hasNull = true;
                    }
                }
                return new ByteArrayBlock(count, hasNull ? Optional.of(nulls) : Optional.empty(), values);
            }

            @Override
            public boolean supportsBatchRead() { return true; }
        };
    }

    public static DocValuesReader forLong(LeafReader reader, String field) throws IOException
    {
        return forLongBlock(reader, field, BigintType.BIGINT, false);
    }

    public static DocValuesReader forInteger(LeafReader reader, String field) throws IOException
    {
        return forIntBlock(reader, field, IntegerType.INTEGER);
    }

    public static DocValuesReader forSmallint(LeafReader reader, String field) throws IOException
    {
        return forShortBlock(reader, field);
    }

    public static DocValuesReader forTinyint(LeafReader reader, String field) throws IOException
    {
        return forByteBlock(reader, field, TinyintType.TINYINT, false);
    }

    public static DocValuesReader forDouble(LeafReader reader, String field) throws IOException
    {
        return forLongBlock(reader, field, DoubleType.DOUBLE, false);
    }

    public static DocValuesReader forReal(LeafReader reader, String field) throws IOException
    {
        return forIntBlock(reader, field, RealType.REAL);
    }

    public static DocValuesReader forBoolean(LeafReader reader, String field) throws IOException
    {
        return forByteBlock(reader, field, BooleanType.BOOLEAN, true);
    }

    public static DocValuesReader forTimestamp(LeafReader reader, String field) throws IOException
    {
        return forLongBlock(reader, field, TimestampType.TIMESTAMP_MILLIS, true);
    }

    public static DocValuesReader forKeyword(LeafReader reader, String field) throws IOException
    {
        // OpenSearch stores keywords as SortedSetDocValues (multi-valued)
        SortedSetDocValues ssdv = reader.getSortedSetDocValues(field);
        if (ssdv != null) {
            // Build dictionary Block ONCE for this segment — all unique terms
            Block dictionaryBlock = buildDictionaryBlock(ssdv);
            // +1 for null ordinal mapping
            int nullOrdinal = (int) ssdv.getValueCount();

            return new DocValuesReader() {
                @Override
                public void read(int docId, BlockBuilder output) throws IOException
                {
                    if (ssdv.advanceExact(docId) && ssdv.docValueCount() > 0) {
                        BytesRef term = ssdv.lookupOrd(ssdv.nextOrd());
                        VarcharType.VARCHAR.writeSlice(output,
                                Slices.wrappedBuffer(term.bytes, term.offset, term.length));
                    } else {
                        output.appendNull();
                    }
                }

                @Override
                public Block readBatch(int[] docIds, int count, Type type) throws IOException
                {
                    // Collect ordinals into int[] — no string lookups!
                    int[] ordinals = new int[count];
                    boolean hasNull = false;
                    for (int i = 0; i < count; i++) {
                        if (ssdv.advanceExact(docIds[i]) && ssdv.docValueCount() > 0) {
                            ordinals[i] = (int) ssdv.nextOrd();
                        } else {
                            ordinals[i] = nullOrdinal; // Map to null position
                            hasNull = true;
                        }
                    }
                    if (hasNull) {
                        // Add a null entry to the dictionary
                        Block dictWithNull = addNullToDictionary(dictionaryBlock);
                        return DictionaryBlock.create(count, dictWithNull, ordinals);
                    }
                    return DictionaryBlock.create(count, dictionaryBlock, ordinals);
                }

                @Override
                public boolean supportsBatchRead()
                {
                    return true;
                }
            };
        }
        // Fallback: try SortedDocValues (single-valued)
        SortedDocValues sdv = reader.getSortedDocValues(field);
        if (sdv != null) {
            return new DocValuesReader() {
                @Override
                public void read(int docId, BlockBuilder output) throws IOException
                {
                    if (sdv.advanceExact(docId)) {
                        BytesRef term = sdv.lookupOrd(sdv.ordValue());
                        VarcharType.VARCHAR.writeSlice(output,
                                Slices.wrappedBuffer(term.bytes, term.offset, term.length));
                    } else {
                        output.appendNull();
                    }
                }
            };
        }
        return null;
    }

    /**
     * Build a VariableWidthBlock containing all unique terms from the SortedSetDocValues.
     * This is the dictionary that DictionaryBlock references via ordinals.
     */
    private static Block buildDictionaryBlock(SortedSetDocValues ssdv) throws IOException
    {
        long valueCount = ssdv.getValueCount();
        // Build offsets and concatenated bytes for all terms
        int totalBytes = 0;
        int[] offsets = new int[(int) valueCount + 1];
        // First pass: compute sizes
        for (long ord = 0; ord < valueCount; ord++) {
            BytesRef term = ssdv.lookupOrd(ord);
            offsets[(int) ord + 1] = offsets[(int) ord] + term.length;
            totalBytes += term.length;
        }
        // Second pass: copy bytes
        byte[] allBytes = new byte[totalBytes];
        for (long ord = 0; ord < valueCount; ord++) {
            BytesRef term = ssdv.lookupOrd(ord);
            System.arraycopy(term.bytes, term.offset, allBytes, offsets[(int) ord], term.length);
        }
        Slice slice = Slices.wrappedBuffer(allBytes);
        return new VariableWidthBlock((int) valueCount, slice, offsets, Optional.empty());
    }

    /**
     * Add a null entry at the end of the dictionary block.
     */
    private static Block addNullToDictionary(Block dictionary)
    {
        int dictSize = dictionary.getPositionCount();
        BlockBuilder builder = VarcharType.VARCHAR.createBlockBuilder(null, dictSize + 1);
        for (int i = 0; i < dictSize; i++) {
            VarcharType.VARCHAR.appendTo(dictionary, i, builder);
        }
        builder.appendNull();
        return builder.build();
    }

    /** Factory dispatcher — creates the right reader for the Trino type. */
    public static DocValuesReader create(Type type, LeafReader reader, String field) throws IOException
    {
        if (type.equals(BigintType.BIGINT)) return forLong(reader, field);
        if (type.equals(IntegerType.INTEGER)) return forInteger(reader, field);
        if (type.equals(SmallintType.SMALLINT)) return forSmallint(reader, field);
        if (type.equals(TinyintType.TINYINT)) return forTinyint(reader, field);
        if (type.equals(DoubleType.DOUBLE)) return forDouble(reader, field);
        if (type.equals(RealType.REAL)) return forReal(reader, field);
        if (type.equals(BooleanType.BOOLEAN)) return forBoolean(reader, field);
        if (type.equals(TimestampType.TIMESTAMP_MILLIS)) return forTimestamp(reader, field);
        // Keyword fields are mapped to VARCHAR
        if (type.equals(VarcharType.VARCHAR)) return forKeyword(reader, field);
        return null; // Not supported for doc_values
    }

    /** Check if doc_values reading is supported for this Trino type. */
    public static boolean isSupported(Type type)
    {
        return type.equals(BigintType.BIGINT)
                || type.equals(IntegerType.INTEGER)
                || type.equals(SmallintType.SMALLINT)
                || type.equals(TinyintType.TINYINT)
                || type.equals(DoubleType.DOUBLE)
                || type.equals(RealType.REAL)
                || type.equals(BooleanType.BOOLEAN)
                || type.equals(TimestampType.TIMESTAMP_MILLIS)
                || type.equals(VarcharType.VARCHAR);
    }
}
