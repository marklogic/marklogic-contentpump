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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * InputSplit with multiple document files combined.
 * 
 * @author jchen
 *
 */
public class CombineDocumentSplit extends InputSplit implements Writable {

    private List<FileSplit> splits;
    private long length;
    private Set<String> locations;
    
    public CombineDocumentSplit() {
        splits = new ArrayList<FileSplit>();
        locations = new HashSet<String>();
    }
    
    public CombineDocumentSplit(List<FileSplit> splits) 
    throws IOException, InterruptedException {
        this.splits = splits;
        locations = new HashSet<String>();
        for (InputSplit split : splits) {
            length += split.getLength();
            for (String loc : split.getLocations()) {
                if (!locations.contains(loc)) {
                    locations.add(loc);
                }
            }
        }
    }
    
    public List<FileSplit> getSplits() {
        return splits;
    }

    public void setSplits(List<FileSplit> splits) {
        this.splits = splits;
    }
    
    public void addSplit(FileSplit split) 
    throws IOException, InterruptedException {
        splits.add(split);
        length += split.getLength();
        for (String loc : split.getLocations()) {
            if (!locations.contains(loc)) {
                locations.add(loc);
            }
        }
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return length;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        if (locations.isEmpty()) {
            return new String[0];
        } else {
            return locations.toArray(new String[locations.size()]);
        }    
    }

    public void readFields(DataInput in) throws IOException {
        // splits
        int splitSize = in.readInt();
        splits = new ArrayList<FileSplit>();
        for (int i = 0; i < splitSize; i++) {
            Path path = new Path(Text.readString(in));
            long start = in.readLong();
            long len = in.readLong();
            FileSplit split = new FileSplit(path, start, len, null);
            splits.add(split);
        }
        // length
        length = in.readLong();
        // locations
        locations = new HashSet<String>();
    }
    
    public void write(DataOutput out) throws IOException {
        // splits
        out.writeInt(splits.size());
        for (FileSplit split : splits) {
            Text.writeString(out, split.getPath().toString());  
            out.writeLong(split.getStart());
            out.writeLong(split.getLength());
        }
        // length
        out.writeLong(length);
        // locations: not needed for serialization.  See FileSplit.write().
    }
}
