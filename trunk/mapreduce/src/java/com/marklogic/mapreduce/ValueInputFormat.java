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
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * <p>MarkLogicInputFormat for a value other than document or node with 
 * user specified key and connector-generated key.</p>
 * 
 * <p>
 *  Use this class to get input data from a MarkLogic database and produce
 *  key-value pairs with value types such as {@link org.apache.hadoop.io.Text}
 *  and {@link org.apache.hadoop.io.IntWritable} through implicit conversions
 *  performed by the connector. For details on the supported conversions, see
 *  "Using KeyValueInputFormat and ValueInputFormat" in the 
 *  <em>Hadoop MapReduce Connector Developer's Guide</em>.
 * </p>
 * <p>
 *  The LongWritable key created with this class is not intended to be a
 *  meaningful value. Use this class when only the content in the value
 *  is interesting. The key is simply the number of values seen at the
 *  time a key-value pair is generated.
 * </p>
 * 
 * @see com.marklogic.mapreduce.examples.LinkCount
 * 
 * @author jchen
 *
 * @param <VALUEIN>
 */
public class ValueInputFormat<VALUEIN> 
extends MarkLogicInputFormat<LongWritable, VALUEIN> 
implements MarkLogicConstants {

    @Override
    public RecordReader<LongWritable, VALUEIN> createRecordReader(
            InputSplit split, TaskAttemptContext context)
    throws IOException, InterruptedException {
        return new ValueReader<VALUEIN>(context.getConfiguration());
    }

}
