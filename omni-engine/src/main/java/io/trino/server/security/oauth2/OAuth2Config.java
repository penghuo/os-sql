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
package io.trino.server.security.oauth2;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigSecuritySensitive;
import io.airlift.configuration.LegacyConfig;
import io.airlift.configuration.validation.FileExists;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;
import jakarta.validation.constraints.NotNull;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.server.security.oauth2.OAuth2Service.OPENID_SCOPE;

public class OAuth2Config
{
    private Optional<String> stateKey = Optional.empty();
    private String issuer;
    private String clientId;
    private String clientSecret;
    private Set<String> scopes = ImmutableSet.of(OPENID_SCOPE);
    private String principalField = "sub";
    private Optional<String> groupsField = Optional.empty();
    private List<String> additionalAudiences = Collections.emptyList();
    private Duration challengeTimeout = new Duration(15, TimeUnit.MINUTES);
    private Duration maxClockSkew = new Duration(1, TimeUnit.MINUTES);
    private Optional<String> jwtType = Optional.empty();
    private Optional<String> userMappingPattern = Optional.empty();
    private Optional<File> userMappingFile = Optional.empty();
    private boolean enableRefreshTokens;
    private boolean enableDiscovery = true;

    public Optional<String> getStateKey()
    {
        return stateKey;
    }

    public OAuth2Config setStateKey(String stateKey)
    {
        this.stateKey = Optional.ofNullable(stateKey);
        return this;
    }

    @NotNull
    public String getIssuer()
    {
        return issuer;
    }

    public OAuth2Config setIssuer(String issuer)
    {
        this.issuer = issuer;
        return this;
    }

    @NotNull
    public String getClientId()
    {
        return clientId;
    }

    public OAuth2Config setClientId(String clientId)
    {
        this.clientId = clientId;
        return this;
    }

    @NotNull
    public String getClientSecret()
    {
        return clientSecret;
    }

    @ConfigSecuritySensitive
    public OAuth2Config setClientSecret(String clientSecret)
    {
        this.clientSecret = clientSecret;
        return this;
    }

    @NotNull
    public List<String> getAdditionalAudiences()
    {
        return additionalAudiences;
    }

    @LegacyConfig("http-server.authentication.oauth2.audience")
    public OAuth2Config setAdditionalAudiences(List<String> additionalAudiences)
    {
        this.additionalAudiences = ImmutableList.copyOf(additionalAudiences);
        return this;
    }

    @NotNull
    public Set<String> getScopes()
    {
        return scopes;
    }

    public OAuth2Config setScopes(String scopes)
    {
        this.scopes = Splitter.on(',').trimResults().omitEmptyStrings().splitToStream(scopes).collect(toImmutableSet());
        return this;
    }

    @NotNull
    public String getPrincipalField()
    {
        return principalField;
    }

    public OAuth2Config setPrincipalField(String principalField)
    {
        this.principalField = principalField;
        return this;
    }

    public Optional<String> getGroupsField()
    {
        return groupsField;
    }

    public OAuth2Config setGroupsField(String groupsField)
    {
        this.groupsField = Optional.ofNullable(groupsField);
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getChallengeTimeout()
    {
        return challengeTimeout;
    }

    public OAuth2Config setChallengeTimeout(Duration challengeTimeout)
    {
        this.challengeTimeout = challengeTimeout;
        return this;
    }

    @MinDuration("0s")
    @NotNull
    public Duration getMaxClockSkew()
    {
        return maxClockSkew;
    }

    public OAuth2Config setMaxClockSkew(Duration maxClockSkew)
    {
        this.maxClockSkew = maxClockSkew;
        return this;
    }

    public Optional<String> getJwtType()
    {
        return jwtType;
    }

    public OAuth2Config setJwtType(String jwtType)
    {
        this.jwtType = Optional.ofNullable(jwtType);
        return this;
    }

    public Optional<String> getUserMappingPattern()
    {
        return userMappingPattern;
    }

    public OAuth2Config setUserMappingPattern(String userMappingPattern)
    {
        this.userMappingPattern = Optional.ofNullable(userMappingPattern);
        return this;
    }

    public Optional<@FileExists File> getUserMappingFile()
    {
        return userMappingFile;
    }

    public OAuth2Config setUserMappingFile(File userMappingFile)
    {
        this.userMappingFile = Optional.ofNullable(userMappingFile);
        return this;
    }

    public boolean isEnableRefreshTokens()
    {
        return enableRefreshTokens;
    }

    public OAuth2Config setEnableRefreshTokens(boolean enableRefreshTokens)
    {
        this.enableRefreshTokens = enableRefreshTokens;
        return this;
    }

    public boolean isEnableDiscovery()
    {
        return enableDiscovery;
    }

    public OAuth2Config setEnableDiscovery(boolean enableDiscovery)
    {
        this.enableDiscovery = enableDiscovery;
        return this;
    }
}
