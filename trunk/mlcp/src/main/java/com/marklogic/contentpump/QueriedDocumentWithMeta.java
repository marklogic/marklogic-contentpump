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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;

import com.marklogic.mapreduce.QueriedDocument;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentPermission;

/**
 * MarkLogicDocument with metadata, usually constructed when importing archive.
 * @author ali
 *
 */
public class QueriedDocumentWithMeta extends QueriedDocument {
    protected DocumentMetadata meta;

    public DocumentMetadata getMeta() {
        return meta;
    }

    public void setMeta(DocumentMetadata meta) {
        this.meta = meta;
    }

    public void updateOptions(ContentCreateOptions options) {
        if (meta == null) {
            return;
        }
        options.setQuality(meta.quality);
        options.setCollections(meta.collectionsList
            .toArray(new String[meta.collectionsList.size()]));
        options.setPermissions(meta.permissionsList
            .toArray(new ContentPermission[meta.permissionsList.size()]));
    }

    public String getProperties() {
        return meta.getProperties();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int len = in.readInt();
        byte[] xml = new byte[len];
        in.readFully(xml, 0, len);
        StringReader reader = new StringReader(new String(xml));
        meta = DocumentMetadata.fromXML(reader);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        byte[] xml = meta.toXML().getBytes();
        out.writeInt(xml.length);
        out.write(xml);
    }

}
