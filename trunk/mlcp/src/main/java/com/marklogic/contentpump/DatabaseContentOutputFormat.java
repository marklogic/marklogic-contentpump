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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.InternalUtilities;
import com.marklogic.mapreduce.LinkedMapWritable;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.exceptions.XccConfigException;

/**
 * ContentOutputFormat for importing archive to MarkLogic and copying from
 * source MarkLogic Server to destination MarkLogic Server.
 * 
 * @author ali
 * 
 */
public class DatabaseContentOutputFormat 
extends ContentOutputFormat<MarkLogicDocumentWithMeta> {
    public static final String ID_PREFIX = "#";
    @Override
    public RecordWriter<DocumentURI, MarkLogicDocumentWithMeta> getRecordWriter(
            TaskAttemptContext context) 
    throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        LinkedMapWritable forestHostMap = getForestHostMap(conf);
         
        boolean fastLoad = conf.getBoolean(OUTPUT_FAST_LOAD, false) ||
            (conf.get(OUTPUT_DIRECTORY) != null);
        Map<String, ContentSource> sourceMap = 
            new LinkedHashMap<String, ContentSource>();
        if (fastLoad) {        
            // get host->contentSource mapping
            Map<Writable, ContentSource> hostSourceMap = 
                new HashMap<Writable, ContentSource>();
            for (Writable hostName : forestHostMap.values()) {
                if (hostSourceMap.get(hostName) == null) {
                    try {
                        ContentSource cs = InternalUtilities.getOutputContentSource(
                            conf, hostName.toString());
                        hostSourceMap.put(hostName, cs);
                    } catch (XccConfigException e) {
                        throw new IOException(e);
                    } 
                }
            }
            
            // consolidate forest->host map and host-contentSource map to 
            // forest-contentSource map
            for (Writable forestId : forestHostMap.keySet()) {
                String forest = ((Text)forestId).toString();
                Writable hostName = forestHostMap.get(forestId);
                ContentSource cs = hostSourceMap.get(hostName);
                sourceMap.put(ID_PREFIX + forest, cs);
            }           
        } else {
            // treating the non-fast-load case as a special case of the 
            // fast-load case with only one content source
            int taskId = context.getTaskAttemptID().getTaskID().getId();
            String host = InternalUtilities.getHost(taskId, forestHostMap);
            
            try {
                ContentSource cs = InternalUtilities.getOutputContentSource(
                    conf, host.toString());
                sourceMap.put(ID_PREFIX, cs);
            } catch (XccConfigException e) {
                throw new IOException(e);
            }
        }
        
        // construct the ContentWriter
        return new DatabaseContentWriter<MarkLogicDocumentWithMeta>(conf, sourceMap, fastLoad);
        
    }
}
