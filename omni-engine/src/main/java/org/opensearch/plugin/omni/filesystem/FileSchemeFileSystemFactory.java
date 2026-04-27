/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.omni.filesystem;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.spi.security.ConnectorIdentity;

import java.nio.file.Path;

/**
 * Wraps LocalFileSystem to accept file:// URIs by converting them to local:// URIs.
 */
public class FileSchemeFileSystemFactory implements TrinoFileSystemFactory
{
    private final LocalFileSystem delegate;

    public FileSchemeFileSystemFactory()
    {
        this.delegate = new LocalFileSystem(Path.of("/"));
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new FileSchemeFileSystem(delegate);
    }
}
