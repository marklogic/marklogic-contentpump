/*
 * Copyright (c) 2024 MarkLogic Corporation
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

import com.marklogic.mapreduce.StreamLocator;

/**
 * Reader for compressed documents from file systems and produce
 * &lt;DocumentURI, Path&gt; pairs. 
 * 
 * @author jchen
 */
public class CompressedStreamingReader 
extends CompressedDocumentReader<StreamLocator> {
    @Override
    protected void setValue(long length) {
        if (value == null || value.getPath() == null) {
            value = new StreamLocator(file, codec);
        }
    }
}
