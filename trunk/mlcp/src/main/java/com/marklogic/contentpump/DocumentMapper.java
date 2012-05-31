/*******************************************************************************
 * Copyright 2003-2012 MarkLogic Corporation
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;

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
    Mapper<DocumentURI, VALUE, DocumentURI, VALUE> {
    public void map(DocumentURI uri, VALUE fileContent, Context context)
        throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();

        Configuration conf = context.getConfiguration();
        String outDir = conf.get(ConfigConstants.CONF_OUTPUT_DIRECTORY);
        if (outDir != null) {
            sb.append(outDir);
        }
        sb.append(uri.toString());
        String[] uriReplace = conf
            .getStrings(ConfigConstants.CONF_OUTPUT_URI_REPLACE);
        int i=0;
        while (uriReplace != null && i < uriReplace.length) {
            int fromIndex = 0;
            while ((fromIndex = sb.indexOf(uriReplace[i], fromIndex)) != -1) {
                sb.replace(fromIndex, fromIndex + uriReplace[0].length(),
                    uriReplace[i + 1]);
            }
            i += 2;
        }
        uri.setUri(sb.toString());
        context.write(uri, fileContent);
    }

}
