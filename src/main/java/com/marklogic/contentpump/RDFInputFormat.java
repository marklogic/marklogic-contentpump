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

package com.marklogic.contentpump;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.DocumentURIWithSourceInfo;
import com.marklogic.mapreduce.LinkedMapWritable;

/**
 * InputFormat for RDF.
 */
public class RDFInputFormat extends 
FileAndDirectoryInputFormat<DocumentURIWithSourceInfo, Text> {
    public static final Log LOG = LogFactory.getLog(RDFInputFormat.class);
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
    
    @Override
    public RecordReader<DocumentURIWithSourceInfo, Text> createRecordReader(InputSplit is,
        TaskAttemptContext context) throws IOException, InterruptedException {
        String version = null;
        LinkedMapWritable roleMap = null;
        try {
            version = getServerVersion(context);
            roleMap = getRoleMap(context);
        } catch (IOException e) {
            throw new IOException("Error creating RecordReader:" + e.getMessage());
        }
        return new RDFReader<>(version, roleMap);
    }
    
    protected LinkedMapWritable getRoleMap(TaskAttemptContext context) throws IOException{
        //Restores the object from the configuration.
        Configuration conf = context.getConfiguration();
        LinkedMapWritable fhmap = null;
        if(conf.get(ConfigConstants.CONF_ROLE_MAP)!=null) {
            fhmap = DefaultStringifier.load(conf, ConfigConstants.CONF_ROLE_MAP, 
                LinkedMapWritable.class);
        }
        return fhmap;
    }
    
    protected String getServerVersion(TaskAttemptContext context) throws IOException{
        //Restores the object from the configuration.
        Configuration conf = context.getConfiguration();
        Text version = DefaultStringifier.load(conf, ConfigConstants.CONF_ML_VERSION, 
            Text.class);
        return version.toString();
    }
}
