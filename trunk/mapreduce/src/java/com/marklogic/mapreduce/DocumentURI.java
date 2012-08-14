/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.Normalizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Document URI, used as a key for a document record. Use with
 * {@link DocumentInputFormat} and {@link ContentOutputFormat}.
 * 
 * @author jchen
 */
public class DocumentURI implements WritableComparable<DocumentURI> {
    private static final long HASH64_STEP = 15485863;
    private static final long HASH64_SEED = 0x39a51471f80aabf7l;
    private static final BigInteger URI_KEY_HASH = hash64("uri()");
    
    private String uri;

    public DocumentURI() {}
    
    public DocumentURI(String uri) {
        this.uri = uri;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        uri = Text.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, uri);
    }

    public String getUri() {
        return uri;
    }
    
    @Deprecated
    public String getUnparsedUri() {
        return InternalUtilities.unparse(uri);
    }
    
    public void setUri(String uri) {
        this.uri = uri;
    }
    
    @Override
    public int compareTo(DocumentURI o) {
        return uri.compareTo(o.getUri());
    }
    
    @Override
    public String toString() {
        return uri;
    }
    
    private static BigInteger hash64(String str) {
        BigInteger value = BigInteger.valueOf(HASH64_SEED);

        for(int cp, i = 0; i < str.length(); i += Character.charCount(cp)) {
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
        return value.shiftLeft(shift).xor(
                value.shiftRight(64-shift)).longValue();
    }
    
    protected void normalize() {
        uri = Normalizer.normalize(uri, Normalizer.Form.NFC);
    }
    
    protected BigInteger getUriKey() {       
        BigInteger value = 
            hash64(uri).multiply(BigInteger.valueOf(5)).add(URI_KEY_HASH);
        byte[] valueBytes = value.toByteArray();
        byte[] longBytes = new byte[8];
        System.arraycopy(valueBytes, valueBytes.length - longBytes.length, 
                longBytes, 0, longBytes.length);
        BigInteger key = new BigInteger(1, longBytes);
        return key;
    }
    
    /**
     * Assign a forest based on the URI key and the number of forests.  Return
     * a zero-based index to the forest list.
     * 
     * @param size size 
     * @return index to the forest list.
     */
    public int getPlacementId(int size) {
        switch (size) {
            case 0: 
                throw new IllegalArgumentException("getPlacementId(size = 0)");
            case 1: return 0;
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
    
    public void validate() {
        if (uri.isEmpty() || 
            Character.isWhitespace(uri.charAt(0)) ||
            Character.isWhitespace(uri.charAt(uri.length() - 1))) {
            throw new IllegalStateException("Invalid URI Format: " + uri);
        }
        for (int i = 0; i < uri.length(); i++) {
            if (uri.charAt(i) < ' ') {
                throw new IllegalStateException("Invalid URI Format: " + uri);
            }
        }
    } 
    
    public static void main(String[] args) throws URISyntaxException {
        for (String arg : args) {
            URI uri = new URI(null, null, null, 0, arg, null, null);
            System.out.println("URI encoded: " + uri.toString());
            URI outuri = new URI(uri.toString());
            System.out.println("URI decoded: " + outuri.getPath());
        }      
    }
}
