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
package com.marklogic.contentpump.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.marklogic.contentpump.SequenceFileValue;
import com.marklogic.mapreduce.MarkLogicNode;

public class SimpleSequenceFileValue<T> implements SequenceFileValue<T>,
    Writable {
    private byte type;
    private T value;

    public void setValue(T v) {
        value = v;
        if (value instanceof Text) {
            type = 0;
        } else if (value instanceof MarkLogicNode) {
            type = 1;
        } else if (value instanceof BytesWritable) {
            type = 2;
        }
    }

    @Override
    public T getValue() {
        return value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput in) throws IOException {
        byte valueType = in.readByte();
        switch (valueType) {
        case 0:
            value = (T) new Text();
            ((Text) value).readFields(in);
            break;
        case 1:
            value = (T) new MarkLogicNode();
            ((MarkLogicNode) value).readFields(in);
            break;
        case 2:
            value = (T) new BytesWritable();
            ((BytesWritable) value).readFields(in);
            break;
        default:
            throw new IOException("incorrect type");
        }
        type = valueType;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(type);
        if (value instanceof Text) {
            ((Text) value).write(out);
        } else if (value instanceof MarkLogicNode) {
            ((MarkLogicNode) value).write(out);
        } else if (value instanceof BytesWritable) {
            ((BytesWritable) value).write(out);
        }
    }

}
