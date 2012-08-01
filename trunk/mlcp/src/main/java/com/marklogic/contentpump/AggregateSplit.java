/*
 * Copyright 2003-2012 MarkLogic Corporation
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

package com.marklogic.contentpump;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Stack;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.sun.org.apache.xerces.internal.util.NamespaceSupport;
import com.sun.org.apache.xerces.internal.xni.NamespaceContext;

public class AggregateSplit extends InputSplit implements Writable {
    private Path file;
    private long start;
    private long length;
    private String[] hosts;
    private HashMap<String, Stack<String>> namespaces;
    private String recordElem;
    private NamespaceContext nsctx;

    AggregateSplit() {
    }

    public AggregateSplit(FileSplit split,
        HashMap<String, Stack<String>> namespaces, String recordElem,
        NamespaceContext nsctx) throws IOException {
        this.file = split.getPath();
        this.start = split.getStart();
        this.length = split.getLength();
        hosts = split.getLocations();
        this.namespaces = namespaces;
        this.recordElem = recordElem;
        this.nsctx = nsctx;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        if (this.hosts == null) {
            return new String[] {};
        } else {
            return this.hosts;
        }
    }

    public HashMap<String, Stack<String>> getNamespaces() {
        return namespaces;

    }

    public String getRecordElem() {
        return recordElem;
    }

    public NamespaceContext getNamespaceContext() {
        return nsctx;
    }

    public void setRecordElem(String recordElem) {
        this.recordElem = recordElem;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, file.toString());
        out.writeLong(start);
        out.writeLong(length);
        Text.writeString(out, recordElem);
        int nscount = ((NamespaceSupport) nsctx).getDeclaredPrefixCount();
        out.writeInt(nscount);
        for (int i = 0; i < nscount; i++) {
            String prefix = ((NamespaceSupport) nsctx).getDeclaredPrefixAt(i);
            Text.writeString(out, prefix);
            Text.writeString(out, ((NamespaceSupport) nsctx).getURI(prefix)
                .trim());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        file = new Path(Text.readString(in));
        start = in.readLong();
        length = in.readLong();
        recordElem = Text.readString(in);
        hosts = null;
        namespaces = new HashMap<String, Stack<String>>();
        int nscount = in.readInt();
        nsctx = new NamespaceSupportAggregate();
        for (int i = 0; i < nscount; i++) {
            String prefix = Text.readString(in);
            String uri = Text.readString(in);
            nsctx.declarePrefix(prefix, uri);
            Stack<String> s = new Stack<String>();
            s.push(uri);
            namespaces.put("".equals(prefix) ? AggregateXMLReader.DEFAULT_NS
                : prefix, s);
        }
    }

    /** The file containing this split's data. */
    public Path getPath() {
        return file;
    }

    /** The position of the first byte in the file to process. */
    public long getStart() {
        return start;
    }
}
