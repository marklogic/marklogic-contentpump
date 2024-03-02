/*
 * Copyright (c) 2023 MarkLogic Corporation
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
package com.marklogic.mapreduce.utilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.marklogic.mapreduce.utilities.ForestHost;


public class ForestInfo implements Writable {
    private String hostName;
    private long frangmentCount;
    private boolean updatable;
    private List<ForestHost> replicas;

    public ForestInfo() {
    }

    public ForestInfo(String hostName, long fmCount,
        boolean updatable, List<ForestHost> replicas) {
        super();
        this.hostName = hostName;
        this.frangmentCount = fmCount;
        this.updatable = updatable;
        this.replicas = new ArrayList<>();
        int start = 0;
        int len = replicas.size();
        /* find the current open forest 
         * use hostname to compare because forest is the same for shared-disk failover
         */
        for (int i = 0; i < len; i++) {
            if (hostName.equals(replicas.get(i).getHostName())) {
                start = i;
                break;
            }
        }
        /* reorder the list so that the open forest is the first in the list
         */
        for (int i = start; i < len; i++) {
            this.replicas.add(replicas.get(i));
        }
        for (int i = 0; i < start; i++) {
            this.replicas.add(replicas.get(i));
        }
    }

    public long getFragmentCount() {
        return frangmentCount;
    }

    public String getHostName() {
        return hostName;
    }

    public boolean getUpdatable() {
        return updatable;
    }

    public List<ForestHost> getReplicas() {
        return replicas;
    }

    public void readFields(DataInput in) throws IOException {
        hostName = Text.readString(in);
        frangmentCount = in.readLong();
        updatable = in.readBoolean();
        int replicaSize = in.readInt();
        replicas = new ArrayList<>();
        for (int i=0; i < replicaSize; i++) {
            String curForest = Text.readString(in);
            String curHost = Text.readString(in);
            ForestHost fh = new ForestHost(curForest, curHost);
            replicas.add(fh);
        }
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, hostName);
        out.writeLong(frangmentCount);
        out.writeBoolean(updatable);
        int replicaSize = replicas.size();
        out.writeInt(replicaSize);
        for (ForestHost replica : replicas) {
            Text.writeString(out, replica.getForest());
            Text.writeString(out, replica.getHostName());
        }
    }

}
