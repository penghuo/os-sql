/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.distributed;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.executor.distributed.ExchangeService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/** Transport action handler for data exchange between plan fragments. */
public class TransportExchangeDataAction
        extends HandledTransportAction<ActionRequest, ExchangeDataResponse> {

    private static final long DEFAULT_BUFFER_BYTES = 32 * 1024 * 1024; // 32 MB

    private final ExchangeService exchangeService;

    @Inject
    public TransportExchangeDataAction(
            TransportService transportService,
            ActionFilters actionFilters,
            ExchangeService exchangeService) {
        super(
                ExchangeDataAction.NAME,
                transportService,
                actionFilters,
                ExchangeDataRequest::new);
        this.exchangeService = exchangeService;
    }

    @Override
    protected void doExecute(
            Task task,
            ActionRequest actionRequest,
            ActionListener<ExchangeDataResponse> listener) {
        ExchangeDataRequest request = (ExchangeDataRequest) actionRequest;
        try {
            boolean accepted =
                    exchangeService.feedResults(
                            request.getQueryId(),
                            request.getTargetFragmentId(),
                            deserializeRows(request.getRows()));

            if (request.isLastBatch()) {
                exchangeService.markAllSourcesComplete(
                        request.getQueryId(), request.getTargetFragmentId());
            }

            listener.onResponse(
                    new ExchangeDataResponse(
                            accepted,
                            accepted ? DEFAULT_BUFFER_BYTES : 0L,
                            request.getSequenceNumber()));
        } catch (Exception e) {
            listener.onResponse(
                    new ExchangeDataResponse(false, 0L, request.getSequenceNumber()));
        }
    }

    /**
     * Serialize a list of ExprValue instances to a byte array using Java serialization.
     *
     * @param rows the rows to serialize
     * @return the serialized bytes
     */
    static byte[] serializeRows(List<ExprValue> rows) {
        if (rows == null || rows.isEmpty()) {
            return new byte[0];
        }
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeInt(rows.size());
            for (ExprValue row : rows) {
                oos.writeObject(row);
            }
            oos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize rows", e);
        }
    }

    /**
     * Deserialize a byte array back to a list of ExprValue instances.
     *
     * @param data the serialized bytes
     * @return the deserialized rows
     */
    static List<ExprValue> deserializeRows(byte[] data) {
        if (data == null || data.length == 0) {
            return Collections.emptyList();
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
                ObjectInputStream ois = new ObjectInputStream(bais)) {
            int size = ois.readInt();
            List<ExprValue> rows = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                rows.add((ExprValue) ois.readObject());
            }
            return rows;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to deserialize rows", e);
        }
    }
}
