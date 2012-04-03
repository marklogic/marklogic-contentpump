/*
 * Copyright 2003-2012 MarkLogic Corporation
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

import org.apache.hadoop.io.Text;

import com.marklogic.mapreduce.DocumentURI;

import org.apache.hadoop.mapreduce.Mapper;

/**
 * Maps (file name, content) as (DocumentURI, Text) to (URI, document) as 
 * (DocumentURI, Text).
 * 
 * @author jchen
 *
 */
public class TextDocumentMapper extends
        Mapper<DocumentURI, Text, DocumentURI, Text> {    
    public void map(DocumentURI uri, Text fileContent, Context context) 
    throws IOException, InterruptedException {
        context.write(uri, fileContent);
    }
}
