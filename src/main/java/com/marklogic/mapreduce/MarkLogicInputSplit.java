/*
 * Copyright 2003-2019 MarkLogic Corporation

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
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import com.marklogic.mapreduce.utilities.ForestHost;

/**
 * MarkLogic-based InputSplit, used to represent a group of records returned by
 * MarkLogic server, and identify originating host.
 * 
 * @author jchen
 */
public class MarkLogicInputSplit extends InputSplit implements Writable {
    /** 
     * beginning offset of the result sequence in a forest
     */
    private long start = 0;
    /**
     *  total count of results in the split
     */
    private long length = 0;
    /**
     *  forest id
     */
    private BigInteger forestId;
    /**
     *  host name
     */
    private String[] hostName;
    /**
     * is the last split in the job
     */
    private boolean isLastSplit;

    private List<ForestHost> replicas;
    
    public MarkLogicInputSplit() {
    }
    
    public MarkLogicInputSplit(long start, long length, BigInteger forestId, 
            String hostName, List<ForestHost> replicas) {
        this.start = start;
        this.length = length;
        this.forestId = forestId;
        this.hostName = new String[1];
        this.hostName[0] = hostName;
        this.replicas = replicas;
    }
    
    @Override
    public long getLength() throws IOException, InterruptedException {
        return length;
    }
    
    /**
     * Is this the last split?
     * @return True if this is the last split; false otherwise.
     */
    public boolean isLastSplit() {
        return isLastSplit;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return hostName;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public BigInteger getForestId() {
        return forestId;
    }

    public void setForestId(BigInteger forestId) {
        this.forestId = forestId;
    }

    public void setHostName(String[] hostName) {
        this.hostName = hostName;
    }

    public void setLength(long length) {
        this.length = length;
    }
    
    public void setLastSplit(boolean isLast) {
        isLastSplit = isLast;
    }

    public List<ForestHost> getReplicas() {
       return replicas;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        start = in.readLong();
        length = in.readLong();
        Text forestIdText = new Text();
        forestIdText.readFields(in);
        forestId = new BigInteger(forestIdText.getBytes());
        hostName = new String[1];
        hostName[0] = Text.readString(in);
        isLastSplit = in.readBoolean();
        int replicaSize = in.readInt();
        replicas = new ArrayList<>();
        for (int i=0; i < replicaSize; i++) {
            String curForest = Text.readString(in);
            String curHost = Text.readString(in);
            ForestHost fh = new ForestHost(curForest, curHost);
            replicas.add(fh);
        } 
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(start);
        out.writeLong(length);
        Text forestIdText = new Text(forestId.toByteArray());
        forestIdText.write(out);
        if (hostName != null && hostName.length > 0) {
            Text.writeString(out, hostName[0]);
        }
        out.writeBoolean(isLastSplit);
        int replicaSize = (replicas != null) ? replicas.size() : 0;
        out.writeInt(replicaSize);
        for (int i=0; i < replicaSize; i++) {
            Text.writeString(out, replicas.get(i).getForest());
            Text.writeString(out, replicas.get(i).getHostName());
        } 
    }

    @Override
    public String toString() {
        return "start: " + start + ", length: " + length + ", forestId: " + 
        forestId + ", hostName: " + 
        (hostName != null && hostName.length > 0 ? hostName[0] : "null");
    }

}
