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
package com.marklogic.contentpump.utilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.mapreduce.utilities.TextArrayWritable;

/**
 * FileSplit for DelimitedText
 */
public class DelimitedSplit extends FileSplit {
    private TextArrayWritable header;

    public DelimitedSplit() {
        // dummy constructor for Reflection used in deserialization
        super(null, 0, 0, null);
        header = new TextArrayWritable();
    }

    public DelimitedSplit(TextArrayWritable header, Path file, long start,
        long length, String[] hosts) {
        super(file, start, length, hosts);
        this.header = header;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        header.readFields(in);
        super.readFields(in);

    }

    @Override
    public void write(DataOutput out) throws IOException {
        header.write(out);
        super.write(out);
    }

    public TextArrayWritable getHeader() {
        return header;
    }

}
