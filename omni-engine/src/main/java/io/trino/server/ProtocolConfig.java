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
package io.trino.server;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.Pattern;

import java.util.Optional;

public class ProtocolConfig
{
    private String alternateHeaderName;
    private int preparedStatementCompressionThreshold = 2 * 1024;
    private int preparedStatementCompressionMinimalGain = 512;

    @Deprecated
    public Optional<@Pattern(regexp = "[A-Za-z]+") String> getAlternateHeaderName()
    {
        return Optional.ofNullable(alternateHeaderName);
    }

    @Deprecated
    public ProtocolConfig setAlternateHeaderName(String alternateHeaderName)
    {
        this.alternateHeaderName = alternateHeaderName;
        return this;
    }

    public int getPreparedStatementCompressionThreshold()
    {
        return preparedStatementCompressionThreshold;
    }

    public ProtocolConfig setPreparedStatementCompressionThreshold(int preparedStatementCompressionThreshold)
    {
        this.preparedStatementCompressionThreshold = preparedStatementCompressionThreshold;
        return this;
    }

    public int getPreparedStatementCompressionMinimalGain()
    {
        return preparedStatementCompressionMinimalGain;
    }

    public ProtocolConfig setPreparedStatementCompressionMinimalGain(int preparedStatementCompressionMinimalGain)
    {
        this.preparedStatementCompressionMinimalGain = preparedStatementCompressionMinimalGain;
        return this;
    }
}
