/*
 * Copyright 2003-2013 MarkLogic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.marklogic.mapreduce.utilities;

import java.math.BigInteger;
import java.text.Normalizer;
import java.util.LinkedHashSet;

import com.marklogic.mapreduce.DocumentURI;

public class LegacyAssignmentPolicy extends AssignmentPolicy {
    private static final long HASH64_STEP = 15485863;
    private static final long HASH64_SEED = 0x39a51471f80aabf7l;
    private static final BigInteger URI_KEY_HASH = hash64("uri()");

    private String uri;
    private String[] forests;

    public LegacyAssignmentPolicy() {
    }

    public LegacyAssignmentPolicy(LinkedHashSet<String> uForests) {
        this.uForests = uForests;
        forests = uForests.toArray(new String[uForests.size()]);
        policy = Kind.LEGACY;
    }

    @Override
    public String getPlacementForestId(DocumentURI uri) {
        if (forests == null) {
            return null;
        }
        this.uri = uri.getUri();
        int fIdx = getPlacementId(uri, forests.length);
        return forests[fIdx];
    }

    public int getPlacementId(DocumentURI uri, int size) {
        this.uri = uri.getUri();
        switch (size) {
        case 0:
            throw new IllegalArgumentException("getPlacementId(size = 0)");
        case 1:
            return 0;
        default:
            normalize();
            BigInteger uriKey = getUriKey();
            long u = uriKey.longValue();
            for (int i = 8; i <= 56; i += 8) {
                u += rotl(uriKey, i);
            }
            long v = (0xffff + size) / size;
            return (int) ((u & 0xffff) / v);
        }
    }

    protected void normalize() {
        uri = Normalizer.normalize(uri, Normalizer.Form.NFC);
    }

    protected BigInteger getUriKey() {
        BigInteger value = hash64(uri).multiply(BigInteger.valueOf(5)).add(
            URI_KEY_HASH);
        byte[] valueBytes = value.toByteArray();
        byte[] longBytes = new byte[8];
        System.arraycopy(valueBytes, valueBytes.length - longBytes.length,
            longBytes, 0, longBytes.length);
        BigInteger key = new BigInteger(1, longBytes);
        return key;
    }

    private static BigInteger hash64(String str) {
        BigInteger value = BigInteger.valueOf(HASH64_SEED);

        for (int cp, i = 0; i < str.length(); i += Character.charCount(cp)) {
            cp = str.codePointAt(i);
            value = value.add(BigInteger.valueOf(cp)).multiply(
                BigInteger.valueOf(HASH64_STEP));
        }
        byte[] valueBytes = value.toByteArray();
        byte[] longBytes = new byte[8];
        System.arraycopy(valueBytes, valueBytes.length - longBytes.length,
            longBytes, 0, longBytes.length);
        BigInteger hash = new BigInteger(1, longBytes);
        return hash;
    }

    private static long rotl(BigInteger value, int shift) {
        return value.shiftLeft(shift).xor(value.shiftRight(64 - shift))
            .longValue();
    }

    @Override
    public int getPlacementForestIndex(DocumentURI uri) {
        return getPlacementId(uri, forests.length);
    }

}
