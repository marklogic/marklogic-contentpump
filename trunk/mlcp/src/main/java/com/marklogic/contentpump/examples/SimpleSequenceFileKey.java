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
package com.marklogic.contentpump.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.marklogic.contentpump.SequenceFileKey;
import com.marklogic.mapreduce.DocumentURI;

public class SimpleSequenceFileKey implements SequenceFileKey, Writable {
    private DocumentURI uri = new DocumentURI();

    public void setDocumentURI(DocumentURI uri) {
        this.uri = uri;
    }

    @Override
    public DocumentURI getDocumentURI() {
        return uri;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        uri.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        uri.write(out);
    }

}
