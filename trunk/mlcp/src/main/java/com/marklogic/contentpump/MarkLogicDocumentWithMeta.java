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

import com.marklogic.mapreduce.MarkLogicDocument;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentPermission;

public class MarkLogicDocumentWithMeta extends MarkLogicDocument {//implements CustomMarkLogicDocument{
    protected DocumentMetadata meta;
    
    public DocumentMetadata getMeta() {
        return meta;
    }

    public void setMeta(DocumentMetadata meta) {
        this.meta = meta;
    }

    public void updateOptions(ContentCreateOptions options){
        // TODO session.setDocumentProperties
        options.setQuality(meta.quality);
        options.setCollections(meta.collectionsList.toArray(new String[0]));
        options.setPermissions(meta.permissionsList.toArray(new ContentPermission[0]));
    }
    
    public String getProperties(){
        return meta.getProperties();
    }
    
    
}
