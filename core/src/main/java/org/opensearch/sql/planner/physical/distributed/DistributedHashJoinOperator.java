/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.distributed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import lombok.Getter;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

/**
 * Distributed hash join operator. Builds a hash table from the build side (smaller child), then
 * streams through the probe side (larger child) to find matches.
 *
 * <p>Supports INNER, LEFT, RIGHT, and CROSS join types.
 */
public class DistributedHashJoinOperator extends PhysicalPlan {

    @Getter private final PhysicalPlan buildChild;
    @Getter private final PhysicalPlan probeChild;
    private final List<Expression> buildKeys;
    private final List<Expression> probeKeys;
    @Getter private final JoinType joinType;

    // Build phase state
    private Map<List<ExprValue>, List<ExprValue>> hashTable;
    private List<String> buildColumns;
    private List<ExprValue> allBuildRows;

    // Probe phase state
    private List<String> probeColumns;
    private ExprValue currentProbeRow;
    private Iterator<ExprValue> currentMatchIterator;
    private boolean currentProbeHadMatch;
    private ExprValue pendingOutput;

    // RIGHT join: track which build keys were matched
    private Set<List<ExprValue>> matchedBuildKeys;
    private Iterator<ExprValue> unmatchedBuildIterator;

    public DistributedHashJoinOperator(
            PhysicalPlan buildChild,
            PhysicalPlan probeChild,
            List<Expression> buildKeys,
            List<Expression> probeKeys,
            JoinType joinType) {
        this.buildChild = buildChild;
        this.probeChild = probeChild;
        this.buildKeys = buildKeys;
        this.probeKeys = probeKeys;
        this.joinType = joinType;
    }

    @Override
    public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
        return visitor.visitDistributedHashJoin(this, context);
    }

    @Override
    public void open() {
        // Phase 1: Build hash table from build child
        buildChild.open();
        hashTable = new HashMap<>();
        allBuildRows = new ArrayList<>();
        buildColumns = null;

        while (buildChild.hasNext()) {
            ExprValue row = buildChild.next();
            if (buildColumns == null) {
                buildColumns = new ArrayList<>(row.tupleValue().keySet());
            }
            allBuildRows.add(row);
            if (joinType != JoinType.CROSS) {
                List<ExprValue> key = extractKey(row, buildKeys);
                if (!containsNullOrMissing(key)) {
                    hashTable.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
                }
            }
        }
        buildChild.close();

        if (buildColumns == null) {
            buildColumns = Collections.emptyList();
        }

        // Phase 2: Prepare probe iteration
        probeChild.open();
        probeColumns = null;
        currentProbeRow = null;
        currentMatchIterator = null;
        currentProbeHadMatch = false;
        pendingOutput = null;

        if (joinType == JoinType.RIGHT) {
            matchedBuildKeys = new HashSet<>();
        }
        unmatchedBuildIterator = null;
    }

    @Override
    public boolean hasNext() {
        if (pendingOutput != null) {
            return true;
        }
        return advance();
    }

    @Override
    public ExprValue next() {
        if (!hasNext()) {
            throw new NoSuchElementException("Hash join exhausted");
        }
        ExprValue result = pendingOutput;
        pendingOutput = null;
        return result;
    }

    @Override
    public void close() {
        probeChild.close();
        hashTable = null;
        allBuildRows = null;
        matchedBuildKeys = null;
    }

    @Override
    public List<PhysicalPlan> getChild() {
        return Arrays.asList(buildChild, probeChild);
    }

    private boolean advance() {
        while (true) {
            // Try to emit next match for current probe row
            if (currentMatchIterator != null && currentMatchIterator.hasNext()) {
                ExprValue buildRow = currentMatchIterator.next();
                pendingOutput = combineRows(currentProbeRow, buildRow);
                currentProbeHadMatch = true;
                return true;
            }

            // LEFT join: emit probe row with null build columns when no match found
            if (currentProbeRow != null && !currentProbeHadMatch
                    && joinType == JoinType.LEFT) {
                pendingOutput = combineWithNullBuild(currentProbeRow);
                currentProbeRow = null;
                return true;
            }

            // Try next probe row
            if (probeChild.hasNext()) {
                currentProbeRow = probeChild.next();
                if (probeColumns == null) {
                    probeColumns = new ArrayList<>(currentProbeRow.tupleValue().keySet());
                }
                currentProbeHadMatch = false;

                if (joinType == JoinType.CROSS) {
                    currentMatchIterator = allBuildRows.iterator();
                } else {
                    List<ExprValue> key = extractKey(currentProbeRow, probeKeys);
                    if (containsNullOrMissing(key)) {
                        currentMatchIterator = Collections.emptyIterator();
                    } else {
                        List<ExprValue> matches = hashTable.get(key);
                        if (matches != null) {
                            currentMatchIterator = matches.iterator();
                            if (matchedBuildKeys != null) {
                                matchedBuildKeys.add(key);
                            }
                        } else {
                            currentMatchIterator = Collections.emptyIterator();
                        }
                    }
                }
                continue;
            }

            // Probe side exhausted: handle RIGHT join unmatched build rows
            if (joinType == JoinType.RIGHT && unmatchedBuildIterator == null) {
                if (probeColumns == null) {
                    probeColumns = Collections.emptyList();
                }
                List<ExprValue> unmatched = new ArrayList<>();
                for (Map.Entry<List<ExprValue>, List<ExprValue>> entry : hashTable.entrySet()) {
                    if (!matchedBuildKeys.contains(entry.getKey())) {
                        unmatched.addAll(entry.getValue());
                    }
                }
                // Build rows with NULL/MISSING keys never match, so include them
                for (ExprValue row : allBuildRows) {
                    List<ExprValue> key = extractKey(row, buildKeys);
                    if (containsNullOrMissing(key)) {
                        unmatched.add(row);
                    }
                }
                unmatchedBuildIterator = unmatched.iterator();
            }

            if (unmatchedBuildIterator != null && unmatchedBuildIterator.hasNext()) {
                ExprValue buildRow = unmatchedBuildIterator.next();
                pendingOutput = combineWithNullProbe(buildRow);
                return true;
            }

            return false;
        }
    }

    private List<ExprValue> extractKey(ExprValue row, List<Expression> keys) {
        List<ExprValue> keyValues = new ArrayList<>(keys.size());
        for (Expression key : keys) {
            keyValues.add(key.valueOf(row.bindingTuples()));
        }
        return keyValues;
    }

    private boolean containsNullOrMissing(List<ExprValue> key) {
        for (ExprValue v : key) {
            if (v.isNull() || v.isMissing()) {
                return true;
            }
        }
        return false;
    }

    private ExprValue combineRows(ExprValue probeRow, ExprValue buildRow) {
        LinkedHashMap<String, ExprValue> combined = new LinkedHashMap<>();
        combined.putAll(probeRow.tupleValue());
        combined.putAll(buildRow.tupleValue());
        return ExprTupleValue.fromExprValueMap(combined);
    }

    private ExprValue combineWithNullBuild(ExprValue probeRow) {
        LinkedHashMap<String, ExprValue> combined = new LinkedHashMap<>();
        combined.putAll(probeRow.tupleValue());
        for (String col : buildColumns) {
            combined.put(col, ExprNullValue.of());
        }
        return ExprTupleValue.fromExprValueMap(combined);
    }

    private ExprValue combineWithNullProbe(ExprValue buildRow) {
        LinkedHashMap<String, ExprValue> combined = new LinkedHashMap<>();
        for (String col : probeColumns) {
            combined.put(col, ExprNullValue.of());
        }
        combined.putAll(buildRow.tupleValue());
        return ExprTupleValue.fromExprValueMap(combined);
    }
}
