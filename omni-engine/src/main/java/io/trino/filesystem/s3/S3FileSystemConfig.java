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
package io.trino.filesystem.s3;

import com.google.common.net.HostAndPort;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MaxDataSize;
import io.airlift.units.MinDataSize;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.util.Optional;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class S3FileSystemConfig
{
    public enum S3SseType
    {
        NONE, S3, KMS
    }

    private String awsAccessKey;
    private String awsSecretKey;
    private String endpoint;
    private String region;
    private boolean pathStyleAccess;
    private String iamRole;
    private String roleSessionName = "trino-filesystem";
    private String externalId;
    private String stsEndpoint;
    private String stsRegion;
    private S3SseType sseType = S3SseType.NONE;
    private String sseKmsKeyId;
    private DataSize streamingPartSize = DataSize.of(16, MEGABYTE);
    private boolean requesterPays;
    private Integer maxConnections;
    private Duration connectionTtl;
    private Duration connectionMaxIdleTime;
    private Duration socketConnectTimeout;
    private Duration socketReadTimeout;
    private boolean tcpKeepAlive;
    private HostAndPort httpProxy;
    private boolean httpProxySecure;

    public String getAwsAccessKey()
    {
        return awsAccessKey;
    }

    public S3FileSystemConfig setAwsAccessKey(String awsAccessKey)
    {
        this.awsAccessKey = awsAccessKey;
        return this;
    }

    public String getAwsSecretKey()
    {
        return awsSecretKey;
    }

    @ConfigSecuritySensitive
    public S3FileSystemConfig setAwsSecretKey(String awsSecretKey)
    {
        this.awsSecretKey = awsSecretKey;
        return this;
    }

    public String getEndpoint()
    {
        return endpoint;
    }

    public S3FileSystemConfig setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
        return this;
    }

    public String getRegion()
    {
        return region;
    }

    public S3FileSystemConfig setRegion(String region)
    {
        this.region = region;
        return this;
    }

    public boolean isPathStyleAccess()
    {
        return pathStyleAccess;
    }

    public S3FileSystemConfig setPathStyleAccess(boolean pathStyleAccess)
    {
        this.pathStyleAccess = pathStyleAccess;
        return this;
    }

    public String getIamRole()
    {
        return iamRole;
    }

    public S3FileSystemConfig setIamRole(String iamRole)
    {
        this.iamRole = iamRole;
        return this;
    }

    @NotNull
    public String getRoleSessionName()
    {
        return roleSessionName;
    }

    public S3FileSystemConfig setRoleSessionName(String roleSessionName)
    {
        this.roleSessionName = roleSessionName;
        return this;
    }

    public String getExternalId()
    {
        return externalId;
    }

    public S3FileSystemConfig setExternalId(String externalId)
    {
        this.externalId = externalId;
        return this;
    }

    public String getStsEndpoint()
    {
        return stsEndpoint;
    }

    public S3FileSystemConfig setStsEndpoint(String stsEndpoint)
    {
        this.stsEndpoint = stsEndpoint;
        return this;
    }

    public String getStsRegion()
    {
        return stsRegion;
    }

    public S3FileSystemConfig setStsRegion(String stsRegion)
    {
        this.stsRegion = stsRegion;
        return this;
    }

    @NotNull
    public S3SseType getSseType()
    {
        return sseType;
    }

    public S3FileSystemConfig setSseType(S3SseType sseType)
    {
        this.sseType = sseType;
        return this;
    }

    public String getSseKmsKeyId()
    {
        return sseKmsKeyId;
    }

    public S3FileSystemConfig setSseKmsKeyId(String sseKmsKeyId)
    {
        this.sseKmsKeyId = sseKmsKeyId;
        return this;
    }

    @NotNull
    @MinDataSize("5MB")
    @MaxDataSize("256MB")
    public DataSize getStreamingPartSize()
    {
        return streamingPartSize;
    }

    public S3FileSystemConfig setStreamingPartSize(DataSize streamingPartSize)
    {
        this.streamingPartSize = streamingPartSize;
        return this;
    }

    public boolean isRequesterPays()
    {
        return requesterPays;
    }

    public S3FileSystemConfig setRequesterPays(boolean requesterPays)
    {
        this.requesterPays = requesterPays;
        return this;
    }

    @Min(1)
    public Integer getMaxConnections()
    {
        return maxConnections;
    }

    public S3FileSystemConfig setMaxConnections(Integer maxConnections)
    {
        this.maxConnections = maxConnections;
        return this;
    }

    public Optional<Duration> getConnectionTtl()
    {
        return Optional.ofNullable(connectionTtl);
    }

    public S3FileSystemConfig setConnectionTtl(Duration connectionTtl)
    {
        this.connectionTtl = connectionTtl;
        return this;
    }

    public Optional<Duration> getConnectionMaxIdleTime()
    {
        return Optional.ofNullable(connectionMaxIdleTime);
    }

    public S3FileSystemConfig setConnectionMaxIdleTime(Duration connectionMaxIdleTime)
    {
        this.connectionMaxIdleTime = connectionMaxIdleTime;
        return this;
    }

    public Optional<Duration> getSocketConnectTimeout()
    {
        return Optional.ofNullable(socketConnectTimeout);
    }

    public S3FileSystemConfig setSocketConnectTimeout(Duration socketConnectTimeout)
    {
        this.socketConnectTimeout = socketConnectTimeout;
        return this;
    }

    public Optional<Duration> getSocketReadTimeout()
    {
        return Optional.ofNullable(socketReadTimeout);
    }

    public S3FileSystemConfig setSocketReadTimeout(Duration socketReadTimeout)
    {
        this.socketReadTimeout = socketReadTimeout;
        return this;
    }

    public boolean getTcpKeepAlive()
    {
        return tcpKeepAlive;
    }

    public S3FileSystemConfig setTcpKeepAlive(boolean tcpKeepAlive)
    {
        this.tcpKeepAlive = tcpKeepAlive;
        return this;
    }

    public HostAndPort getHttpProxy()
    {
        return httpProxy;
    }

    public S3FileSystemConfig setHttpProxy(HostAndPort httpProxy)
    {
        this.httpProxy = httpProxy;
        return this;
    }

    public boolean isHttpProxySecure()
    {
        return httpProxySecure;
    }

    public S3FileSystemConfig setHttpProxySecure(boolean httpProxySecure)
    {
        this.httpProxySecure = httpProxySecure;
        return this;
    }
}
