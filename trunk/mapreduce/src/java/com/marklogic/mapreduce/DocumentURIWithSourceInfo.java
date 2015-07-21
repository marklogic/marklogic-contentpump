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

/**
 * Document URI with source information. Use with
 * {@link DocumentInputFormat} and {@link ContentOutputFormat}.
 * 
 * @author jchen
 */

public class DocumentURIWithSourceInfo extends DocumentURI {

    private String srcId = null;
    private String subId = null;
    private int lineNumber = -1;
    private int colNumber = -1;
    
    DocumentURIWithSourceInfo() {};
    
    DocumentURIWithSourceInfo(String uri, String src) {
        this.uri = uri;
        srcId = src;
    }
    
    DocumentURIWithSourceInfo(String uri, String src, String sub) {
        this.uri = uri;
        srcId = src;
        subId = sub;
    }
    
    DocumentURIWithSourceInfo(String uri, String src, String sub, int line,
            int col) {
        this.uri = uri;
        srcId = src;
        subId = sub;
        lineNumber = line;
        colNumber = col;
    }
    
    DocumentURIWithSourceInfo(DocumentURIWithSourceInfo uri) {
        this.uri = uri.uri;
        this.srcId = uri.srcId;
        this.subId = uri.subId;
        this.lineNumber = uri.lineNumber;
        this.colNumber = uri.colNumber;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        uri = Text.readString(in);
        srcId = Text.readString(in);
        subId = Text.readString(in);
        lineNumber = in.readInt();
        colNumber = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, uri);
        Text.writeString(out, srcId);
        Text.writeString(out, subId);
        out.writeInt(lineNumber);
        out.writeInt(colNumber);
    }

    public String getSrcId() {
        return srcId;
    }

    public void setSrcId(String srcId) {
        this.srcId = srcId;
    }

    public String getSubId() {
        return subId;
    }

    public void setSubId(String subId) {
        this.subId = subId;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    public void setLineNumber(int lineNumber) {
        this.lineNumber = lineNumber;
    }

    public int getColNumber() {
        return colNumber;
    }

    public void setColNumber(int colNumber) {
        this.colNumber = colNumber;
    }
    
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append(uri);
        if (subId != null) {
            buf.append(" from ").append(subId);
        }
        if (srcId != null) {
            buf.append(" in ").append(srcId);
        }
        if (lineNumber != -1) {
            buf.append(" at line ").append(lineNumber);
        }
        if (colNumber != -1) {
            buf.append(" column ").append(colNumber);
        }
        return buf.toString();
    }
}
