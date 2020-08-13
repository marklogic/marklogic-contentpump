/*
 * Copyright 2003-2019 MarkLogic Corporation
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
import java.io.InputStream;

import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;

/**
 * A {@link ForestDocument} representing a binary document in MarkLogic
 * accessed via Direct Access.
 * 
 * @see ForestInputFormat
 * @author jchen
 */
public abstract class BinaryDocument extends ForestDocument {
    @Override
    public Content createContent(String uri, ContentCreateOptions options,
            boolean copyCollections, boolean copyMetadata, boolean copyQuality)
    throws IOException {
        if (copyCollections || copyMetadata || copyQuality) {
            setContentOptions(options, copyCollections, copyMetadata, 
                    copyQuality);
        }
        if (isStreamable()) {
            InputStream is = null;
            try {
                is = getContentAsByteStream();
                return ContentFactory.newUnBufferedContent(uri, is, 
                        options);
            } catch (Exception ex) {
                if (is != null) {
                    is.close();
                } 
                throw new IOException("Error accessing large binary document "
                        + uri + ", skipping...", ex);
            } 
        } else {
            return  ContentFactory.newContent(uri, 
                    getContentAsByteArray(), options);
        }
    }
}
