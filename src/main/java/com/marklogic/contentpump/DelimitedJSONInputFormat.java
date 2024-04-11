/*
 * Copyright (c) 2021 MarkLogic Corporation
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.DocumentURIWithSourceInfo;

/**
 * @author mattsun
 *
 */
public class DelimitedJSONInputFormat extends 
FileAndDirectoryInputFormat<DocumentURIWithSourceInfo, Text> {
    public static final Log LOG = LogFactory.getLog(
            DelimitedJSONInputFormat.class);

    @Override
    public RecordReader<DocumentURIWithSourceInfo, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) 
            throws IOException, InterruptedException {
        return new DelimitedJSONReader<>();
    }

}
