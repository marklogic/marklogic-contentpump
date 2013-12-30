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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;

/**
 * This is a Writable Interface for custom content.
 *
 * Use this class with ContentOutputFormat and your own Reader to
 * load documents into MarkLogic Server with finer grained control
 * over permissions, properties, collections, and content quality
 * than is provided by the per-job configuration properties such
 * as mapreduce.marklogic.output.content.permission.
 *
 * @author ali
 */
public interface CustomContent extends Writable{
    /**
     * Get the content that is about to inserted.  
     * 
     * @param conf job configuration
     * @param options a template for ContentCreateOptions to be used for 
     * the content to be created.
     * @param uri URI String of DocumentURI
     * @return Content
     */
    public Content getContent(Configuration conf, ContentCreateOptions options,
                    String uri);
}
