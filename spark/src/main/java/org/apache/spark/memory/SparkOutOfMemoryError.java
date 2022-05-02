/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.apache.spark.memory;

import org.apache.spark.SparkThrowable;
import org.apache.spark.SparkThrowableHelper;
import org.apache.spark.annotation.Private;

/**
 * This exception is thrown when a task can not acquire memory from the Memory manager.
 * Instead of throwing {@link OutOfMemoryError}, which kills the executor,
 * we should use throw this exception, which just kills the current task.
 */
@Private
public final class SparkOutOfMemoryError extends OutOfMemoryError implements SparkThrowable {
    String errorClass;
    String[] messageParameters;

    public SparkOutOfMemoryError(String s) {
        super(s);
    }

    public SparkOutOfMemoryError(OutOfMemoryError e) {
        super(e.getMessage());
    }

    public SparkOutOfMemoryError(String errorClass, String[] messageParameters) {
        super(SparkThrowableHelper.getMessage(errorClass, messageParameters));
        this.errorClass = errorClass;
        this.messageParameters = messageParameters;
    }

    public String getErrorClass() {
        return errorClass;
    }

    public String getSqlState() {
        return SparkThrowableHelper.getSqlState(errorClass);
    }
}
