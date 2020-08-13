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
package com.marklogic.contentpump;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.WritableUtils;

import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.DatabaseDocument;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentPermission;

/**
 * MarkLogicDocument with metadata, usually constructed when importing archive.
 * @author ali
 *
 */
public class DatabaseDocumentWithMeta extends DatabaseDocument {
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
        int ordinal = in.readInt();
        contentType = ContentType.valueOf(ordinal);
        int contentLen = WritableUtils.readVInt(in);
        if (0 != contentLen) {
            content = new byte[contentLen];
            in.readFully(content, 0, contentLen);
        } else {
            content = new byte[0];
        }
        int len = in.readInt();
        if (0 != len) {
            byte[] xml = new byte[len];
            in.readFully(xml, 0, len);
            StringReader reader = new StringReader(new String(xml));
            meta = DocumentMetadata.fromXML(reader);
        }        
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(contentType.ordinal());
        if (null != content) {
            WritableUtils.writeVInt(out, content.length);
            out.write(content, 0, content.length);
        } else {
            WritableUtils.writeVInt(out, 0);
        }
        if (meta != null) {
            byte[] xml = meta.toXML().getBytes();
            out.writeInt(xml.length);
            out.write(xml);
        } else {
            out.writeInt(0);
        }
    }
}
