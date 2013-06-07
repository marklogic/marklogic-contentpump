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
package com.marklogic.contentpump;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.contentpump.DatabaseContentOutputFormat;
import com.marklogic.contentpump.DatabaseTransformWriter;
import com.marklogic.contentpump.MarkLogicDocumentWithMeta;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.xcc.ContentSource;


public class DatabaseTransformOutputFormat extends DatabaseContentOutputFormat {
    @Override
    public RecordWriter<DocumentURI, MarkLogicDocumentWithMeta> getRecordWriter(
        TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        fastLoad = Boolean.valueOf(conf.get(OUTPUT_FAST_LOAD));
        Map<String, ContentSource> sourceMap = getSourceMap(fastLoad, context);
        // construct the DatabaseTransformContentWriter
        return new DatabaseTransformWriter<MarkLogicDocumentWithMeta>(conf,
            sourceMap, fastLoad, am);
    }
}
