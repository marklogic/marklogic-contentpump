/*
 * Copyright (c) 2021 MarkLogic Corporation

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
package com.marklogic.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.mapreduce.utilities.LegacyAssignmentPolicy;

/**
 * Document URI, used as a key for a document record. Use with
 * {@link DocumentInputFormat} and {@link ContentOutputFormat}.
 * 
 * @author jchen
 */
public class DocumentURI 
implements WritableComparable<DocumentURI>, Cloneable {

    
    protected String uri;

    public DocumentURI() {}
    
    public DocumentURI(String uri) {
        this.uri = uri;
    }
    
    public DocumentURI(DocumentURI uri) {
        this.uri = uri.uri;
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
        this.uri = uri == null ? "" : uri;
    }
    
    @Override
    public int compareTo(DocumentURI o) {
        return uri.compareTo(o.getUri());
    }
    
    @Override
    public String toString() {
        return uri;
    }
    
    /**
     * Assign a forest based on the URI key and the number of forests.  Return
     * a zero-based index to the forest list.
     * 
     * @param size size 
     * @return index to the forest list.
     */
    @Deprecated
    public int getPlacementId(int size) {
        switch (size) {
            case 0: 
                throw new IllegalArgumentException("getPlacementId(size = 0)");
            case 1: return 0;
            default:
                return LegacyAssignmentPolicy.getPlacementId(this, size);
        }
    }
    
    @Override
    public Object clone() {
        return new DocumentURI(uri);
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
    
    @Override
    public boolean equals(Object uri) {
        if (uri instanceof DocumentURI) {
            return this.uri.equals(((DocumentURI)uri).getUri());
        } 
        return false;
    }
    
    @Override
    public int hashCode() {
        return uri.hashCode();
    }
    
    public static void main(String[] args) throws URISyntaxException {
        HashMap<String, DocumentURI> map = new HashMap<>();
        for (String arg : args) {
            URI uri = new URI(null, null, null, 0, arg, null, null);
            System.out.println("URI encoded: " + uri.toString());
            URI outuri = new URI(uri.toString());
            System.out.println("URI decoded: " + outuri.getPath());
            map.put(arg, new DocumentURI(arg));
        }  
    }
}
