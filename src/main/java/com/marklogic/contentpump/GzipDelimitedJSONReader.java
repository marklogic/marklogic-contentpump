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
package com.marklogic.contentpump;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.zip.GZIPInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * @author mattsun
 *
 */
public class GzipDelimitedJSONReader extends DelimitedJSONReader<Text> {
    public static final Log LOG = LogFactory
            .getLog(GzipDelimitedJSONReader.class);
    
    private InputStream gzipIn;
    
    @Override
    protected void initFileStream(InputSplit inSplit) 
            throws IOException, InterruptedException {
        fileIn = openFile(inSplit, true);
        if (fileIn == null) {
            return;
        }
        gzipIn = new GZIPInputStream(fileIn);
        instream = new InputStreamReader(gzipIn, encoding);
        reader = new LineNumberReader(instream);
    }
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return hasNext?0:1;
    }
    
    @Override
    public void close() throws IOException {
        super.close();
        if (gzipIn != null) {
            gzipIn.close();
        }
    }
    
}
