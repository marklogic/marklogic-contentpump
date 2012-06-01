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

import java.io.Reader;
import java.util.Collection;
import java.util.List;
import java.util.Vector;

import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.DocumentFormat;
import com.marklogic.xcc.exceptions.UnimplementedFeatureException;
import com.marklogic.xcc.types.XSInteger;
import com.thoughtworks.xstream.XStream;

public class DocumentMetadata {
    static XStream xstream = new XStream();

    DocumentFormat format = DocumentFormat.XML;

    List<String> collectionsList = new Vector<String>();

    List<ContentPermission> permissionsList = new Vector<ContentPermission>();

    int quality = 0;

    String properties = null;

    /**
     * @param reader
     */
    public static DocumentMetadata fromXML(Reader reader) {
        return (DocumentMetadata) xstream.fromXML(reader);
    }

    /**
     * @return
     */
    public boolean isBinary() {
        return DocumentFormat.BINARY.toString().equals(format.toString());
    }

    /**
     * @param _format
     */
    public void setFormat(DocumentFormat _format) {
        format = _format;
    }

    /**
     * @param _collection
     */
    public void addCollection(String _collection) {
        collectionsList.add(_collection);
    }

    /**
     * @param _permission
     */
    public void addPermission(ContentPermission _permission) {
        permissionsList.add(_permission);
    }

    /**
     * @param _quality
     */
    public void setQuality(int _quality) {
        quality = _quality;
    }

    /**
     * @param _properties
     */
    public void setProperties(String _properties) {
        properties = _properties;
    }

    /**
     * @return
     */
    public String[] getCollections() {
        return collectionsList.toArray(new String[0]);
    }

    /**
     * @return
     */
    public String getProperties() {
        return properties;
    }

    /**
     * @param permissions
     */
    public void addPermissions(Collection<ContentPermission> permissions) {
        if (permissions == null) {
            return;
        }
        permissionsList.addAll(permissions);
    }

    /**
     * @return
     */
    public ContentPermission[] getPermissions() {
        if (permissionsList.size() < 1) {
            return null;
        }
        return permissionsList.toArray(new ContentPermission[0]);
    }

    /**
     * @return
     */
    public int getQuality() {
        return quality;
    }

    /**
     * @return
     */
    public DocumentFormat getFormat() {
        return format;
    }

    /**
     * @return
     */
    public String toXML() {
        // note that this will escape the properties... do we care? no.
        return xstream.toXML(this);
    }

    /**
     *
     */
    public void clearPermissions() {
        permissionsList.clear();
    }

    /**
     *
     */
    public void clearProperties() {
        properties = null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.marklogic.ps.xqsync.MetadataInterface#getFormatName()
     */
    public String getFormatName() {
        return format.toString();
    }

    /**
     * @param _format
     */
    public void setFormat(String _format) {
        if (_format.equals(DocumentFormat.XML)
                || _format.equals("element") || _format.equals("comment")
                || _format.equals("processing-instruction")) {
            setFormat(DocumentFormat.XML);
            return;
        }

        if (_format.equals(DocumentFormat.TEXT)
                || _format.equals(("text"))) {
            setFormat(DocumentFormat.TEXT);
            return;
        }

        // default
        setFormat(DocumentFormat.BINARY);
    }

    /**
     * @param _capability
     * @param _role
     */
    public void addPermission(String _capability, String _role) {
        ContentCapability capability;
        if (ContentPermission.UPDATE.toString().equals(_capability))
            capability = ContentPermission.UPDATE;
        else if (ContentPermission.INSERT.toString().equals(_capability))
            capability = ContentPermission.INSERT;
        else if (ContentPermission.EXECUTE.toString().equals(_capability))
            capability = ContentPermission.EXECUTE;
        else if (ContentPermission.READ.toString().equals(_capability))
            capability = ContentPermission.READ;
        else
            throw new UnimplementedFeatureException(
                    "unknown capability: " + _capability);

        addPermission(new ContentPermission(capability, _role));
    }

    /**
     * @param integer
     */
    public void setQuality(XSInteger integer) {
        setQuality(integer.asPrimitiveInt());
    }

    /**
     * @return
     */
    public boolean isText() {
        return DocumentFormat.TEXT.toString().equals(format.toString());
    }

    /**
     * @return
     */
    public boolean isXml() {
        return DocumentFormat.XML.toString().equals(format.toString());
    }

    /**
     * @param _collections
     */
    public void addCollections(String[] _collections) {
        if (null == _collections || 1 > _collections.length) {
            return;
        }
        for (int i = 0; i < _collections.length; i++) {
            addCollection(_collections[i]);
        }
    }
}
