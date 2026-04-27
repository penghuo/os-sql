/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.omni.filesystem;

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Delegates to a local:// TrinoFileSystem, converting file:// URIs to local:// URIs
 * and converting returned local:// locations back to file://.
 */
class FileSchemeFileSystem implements TrinoFileSystem
{
    private final TrinoFileSystem delegate;

    FileSchemeFileSystem(TrinoFileSystem delegate)
    {
        this.delegate = delegate;
    }

    private static Location toLocal(Location location)
    {
        String uri = location.toString();
        if (uri.startsWith("file://")) {
            return Location.of("local://" + uri.substring("file://".length()));
        }
        return location;
    }

    private static Location toFile(Location location)
    {
        String uri = location.toString();
        if (uri.startsWith("local://")) {
            return Location.of("file://" + uri.substring("local://".length()));
        }
        return location;
    }

    @Override public TrinoInputFile newInputFile(Location location) { return delegate.newInputFile(toLocal(location)); }
    @Override public TrinoInputFile newInputFile(Location location, long length) { return delegate.newInputFile(toLocal(location), length); }
    @Override public TrinoOutputFile newOutputFile(Location location) { return delegate.newOutputFile(toLocal(location)); }
    @Override public void deleteFile(Location location) throws IOException { delegate.deleteFile(toLocal(location)); }
    @Override public void deleteFiles(Collection<Location> locations) throws IOException { delegate.deleteFiles(locations.stream().map(FileSchemeFileSystem::toLocal).collect(Collectors.toList())); }
    @Override public void deleteDirectory(Location location) throws IOException { delegate.deleteDirectory(toLocal(location)); }
    @Override public void renameFile(Location source, Location target) throws IOException { delegate.renameFile(toLocal(source), toLocal(target)); }

    @Override
    public FileIterator listFiles(Location location) throws IOException
    {
        FileIterator inner = delegate.listFiles(toLocal(location));
        return new FileIterator()
        {
            @Override public boolean hasNext() throws IOException { return inner.hasNext(); }
            @Override public FileEntry next() throws IOException {
                FileEntry e = inner.next();
                return new FileEntry(toFile(e.location()), e.length(), e.lastModified(), e.blocks());
            }
        };
    }

    @Override public Optional<Boolean> directoryExists(Location location) throws IOException { return delegate.directoryExists(toLocal(location)); }
    @Override public void createDirectory(Location location) throws IOException { delegate.createDirectory(toLocal(location)); }
    @Override public void renameDirectory(Location source, Location target) throws IOException { delegate.renameDirectory(toLocal(source), toLocal(target)); }

    @Override
    public Set<Location> listDirectories(Location location) throws IOException
    {
        return delegate.listDirectories(toLocal(location)).stream()
                .map(FileSchemeFileSystem::toFile)
                .collect(Collectors.toSet());
    }

    @Override public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix) throws IOException { return delegate.createTemporaryDirectory(toLocal(targetPath), temporaryPrefix, relativePrefix).map(FileSchemeFileSystem::toFile); }
}
