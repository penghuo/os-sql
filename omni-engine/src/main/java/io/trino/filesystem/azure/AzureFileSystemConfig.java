/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.filesystem.azure;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import jakarta.validation.constraints.NotNull;

public class AzureFileSystemConfig
{
    public enum AuthType
    {
        ACCESS_KEY,
        OAUTH,
        DEFAULT,
    }

    private AuthType authType = AuthType.DEFAULT;

    private DataSize readBlockSize = DataSize.of(4, Unit.MEGABYTE);
    private DataSize writeBlockSize = DataSize.of(4, Unit.MEGABYTE);
    private int maxWriteConcurrency = 8;
    private DataSize maxSingleUploadSize = DataSize.of(4, Unit.MEGABYTE);

    @NotNull
    public AuthType getAuthType()
    {
        return authType;
    }

    public AzureFileSystemConfig setAuthType(AuthType authType)
    {
        this.authType = authType;
        return this;
    }

    @NotNull
    public DataSize getReadBlockSize()
    {
        return readBlockSize;
    }

    public AzureFileSystemConfig setReadBlockSize(DataSize readBlockSize)
    {
        this.readBlockSize = readBlockSize;
        return this;
    }

    @NotNull
    public DataSize getWriteBlockSize()
    {
        return writeBlockSize;
    }

    public AzureFileSystemConfig setWriteBlockSize(DataSize writeBlockSize)
    {
        this.writeBlockSize = writeBlockSize;
        return this;
    }

    @NotNull
    public int getMaxWriteConcurrency()
    {
        return maxWriteConcurrency;
    }

    public AzureFileSystemConfig setMaxWriteConcurrency(int maxWriteConcurrency)
    {
        this.maxWriteConcurrency = maxWriteConcurrency;
        return this;
    }

    @NotNull
    public DataSize getMaxSingleUploadSize()
    {
        return maxSingleUploadSize;
    }

    public AzureFileSystemConfig setMaxSingleUploadSize(DataSize maxSingleUploadSize)
    {
        this.maxSingleUploadSize = maxSingleUploadSize;
        return this;
    }
}
