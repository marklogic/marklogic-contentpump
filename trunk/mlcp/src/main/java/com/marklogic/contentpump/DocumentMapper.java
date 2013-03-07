/*******************************************************************************
 * Copyright 2003-2013 MarkLogic Corporation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.marklogic.contentpump;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Counter;

import com.marklogic.mapreduce.DocumentURI;

/**
 * Maps (file name, content) as (DocumentURI, VALUE) to (URI, document) as
 * (DocumentURI, VALUE).
 * 
 * @author ali
 * 
 * @param <VALUE>
 */
public class DocumentMapper<VALUE> extends
    BaseMapper<DocumentURI, VALUE, DocumentURI, VALUE> {
    
    protected Counter inputRecordCount;
    protected Counter skippedRecordCount;
    
    public void map(DocumentURI uri, VALUE fileContent, Context context)
        throws IOException, InterruptedException {
        if (uri == null) {
            skippedRecordCount.increment(1);
            return;
        } else {
            inputRecordCount.increment(1);
        }
        context.write(uri, fileContent);
    }
    
    @Override
    public void setup(Context context) {
        inputRecordCount = context.getCounter(
                        ContentPumpStats.ATTEMPTED_INPUT_RECORD_COUNT);
        skippedRecordCount = context.getCounter(
                        ContentPumpStats.SKIPPED_INPUT_RECORD_COUNT);
    }

}
