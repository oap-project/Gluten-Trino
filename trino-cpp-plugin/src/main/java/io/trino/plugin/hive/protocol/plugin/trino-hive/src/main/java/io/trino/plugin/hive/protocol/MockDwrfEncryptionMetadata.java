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

package io.trino.plugin.hive.protocol;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class MockDwrfEncryptionMetadata
        implements MockEncryptionMetadata
{
    private final Map<String, byte[]> fieldToKeyData;
    private final Map<String, String> extraMetadata;
    private final String encryptionAlgorithm;
    private final String encryptionProvider;

    @JsonCreator
    public MockDwrfEncryptionMetadata(
            @JsonProperty Map<String, byte[]> fieldToKeyData,
            @JsonProperty Map<String, String> extraMetadata,
            @JsonProperty String encryptionAlgorithm,
            @JsonProperty String encryptionProvider)
    {
        this.fieldToKeyData = ImmutableMap.copyOf(requireNonNull(fieldToKeyData, "fieldToKeyData is null"));
        this.extraMetadata = ImmutableMap.copyOf(requireNonNull(extraMetadata, "extraMetadata is null"));
        this.encryptionAlgorithm = requireNonNull(encryptionAlgorithm, "encryptionAlgorithm is null");
        this.encryptionProvider = requireNonNull(encryptionProvider, "encryptionProvider is null");
    }

    @JsonProperty
    public Map<String, byte[]> getFieldToKeyData()
    {
        return fieldToKeyData;
    }

    @JsonProperty
    public Map<String, String> getExtraMetadata()
    {
        return extraMetadata;
    }

    @JsonProperty
    public String getEncryptionAlgorithm()
    {
        return encryptionAlgorithm;
    }

    @JsonProperty
    public String getEncryptionProvider()
    {
        return encryptionProvider;
    }
}
