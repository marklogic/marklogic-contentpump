/*
 * Copyright 2003-2016 MarkLogic Corporation
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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class ForestInfo implements Writable {
    private String hostName;
    private long frangmentCount;
    private boolean updatable;

    public ForestInfo() {
    }

    public ForestInfo(String hostName, long fmCount,
        boolean updatable) {
        super();
        this.hostName = hostName;
        this.frangmentCount = fmCount;
        this.updatable = updatable;
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

    @Override
    public void readFields(DataInput in) throws IOException {
        hostName = Text.readString(in);
        frangmentCount = in.readLong();
        updatable = in.readBoolean();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, hostName);
        out.writeLong(frangmentCount);
        out.writeBoolean(updatable);
    }

}
