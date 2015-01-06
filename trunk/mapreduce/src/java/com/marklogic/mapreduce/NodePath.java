/*
 * Copyright 2003-2015 MarkLogic Corporation

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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import com.marklogic.mapreduce.utilities.InternalUtilities;

/**
 * Node path usable as a key for a node record. Use with
 * {@link NodeInputFormat} and {@link NodeOutputFormat}.
 * 
 * @author jchen
 */
public class NodePath implements WritableComparable<NodePath> {
    // TODO: revisit -- is it faster to use Text or String?
    private String docUri;

    private String path; // relative path
    
    public NodePath() {}
    
    public NodePath(String uri, String path) {
        docUri = uri;
        this.path = path;
    }
    
    public void set(String uri, String path) {
        docUri = uri;
        this.path = path;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        docUri = Text.readString(in);
        path = Text.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, docUri);
        Text.writeString(out, path);
    }

    public String getDocumentUri() {
        return docUri;
    }
    
    public void setDocumentUri(String docUri) {
        this.docUri = docUri;
    }
    
    public String getRelativePath() {
        return path;
    }
    
    public void setRelativePath(String path) {
        this.path = path;
    }
    
    public String getFullPath() {
        StringBuilder buf = new StringBuilder();
        buf.append("fn:doc(\"");
        buf.append(InternalUtilities.unparse(docUri));
        buf.append("\")");
        buf.append(path);
        return buf.toString();
    }

    @Override
    public int compareTo(NodePath o) {
        return (docUri + path).compareTo(o.getDocumentUri() + o.getRelativePath());
    }
}
