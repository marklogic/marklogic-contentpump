/*
 * Copyright 2003-2014 MarkLogic Corporation
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

/**
 * RecordReader for unbuffered streaming documents.  Used when streaming
 * option is set to true.
 */
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.mapreduce.CompressionCodec;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.StreamLocator;

public class StreamingDocumentReader extends 
CombineDocumentReader<StreamLocator> {

    @Override
    public DocumentURI getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (iterator.hasNext()) {
            FileSplit split = iterator.next();
            Path path = split.getPath();
            String uri = makeURIFromPath(path);
            if (uri != null) {
                setKey(uri);
            } else {
                key = null;
                return true;
            }   
            value = new StreamLocator(path, CompressionCodec.NONE);
            bytesRead += split.getLength();
            return true;
        }
        return false;
    }
}
