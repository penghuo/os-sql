/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.apache.spark.shuffle.api.metadata;

import java.io.Serializable;

/**
 * :: Private ::
 * An opaque metadata tag for registering the result of committing the output of a
 * shuffle map task.
 * <p>
 * All implementations must be serializable since this is sent from the executors to
 * the driver.
 */
public interface MapOutputMetadata extends Serializable {}
