/*
 * Copyright 2003-2016 MarkLogic Corporation
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
import java.util.Map;
import java.util.Vector;

import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.DocumentFormat;
import com.marklogic.xcc.exceptions.UnimplementedFeatureException;
import com.marklogic.xcc.types.XSInteger;
import com.thoughtworks.xstream.XStream;

/**
 * Metadata of a MarkLogicDocument, includes properties, permissions, quality
 * and collections.
 * 
 * @author ali
 * 
 */
public class DocumentMetadata {
    static XStream xstream = new XStream();
    /**
     * suffix of the URI of metadata
     */
    public static String EXTENSION = ".metadata";
    /**
     * suffix of the URI of naked properties
     */
    public static String NAKED = ".naked";
    protected DocumentFormat format = DocumentFormat.XML;

    protected List<String> collectionsList = new Vector<String>();

    protected List<ContentPermission> permissionsList = new Vector<>();
    protected String permString = null;
    protected int quality = 0;
    protected Map<String, String> meta = null;

    protected String properties = null;

    protected boolean isNakedProps;
    
    public boolean isNakedProps() {
        return isNakedProps;
    }

    public void setNakedProps(boolean isNakedProps) {
        this.isNakedProps = isNakedProps;
    }

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
        return collectionsList.toArray(new String[collectionsList.size()]);
    }

    public String getCollectionString() {
        if (collectionsList.isEmpty()) {
            return "";
        }
        StringBuilder buf = new StringBuilder();
        buf.append("[");
        for (int i = 0; i < collectionsList.size(); i++) {
            if (i > 0) {
                buf.append(",");
            }
            buf.append("\"").append(collectionsList.get(i)).append("\"");      
        }
        buf.append("]");
        return buf.toString();
    }
    
    public Map<String, String> getMeta() {
        return meta;
    }

    public void setMeta(Map<String, String> meta) {
        this.meta = meta;
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
            return new ContentPermission[]{};
        }
        return permissionsList.toArray(new ContentPermission[permissionsList.size()]);
    }

    /**
     * @return
     */
    public int getQuality() {
        return quality;
    }

    public String getQualityString() {
        return String.valueOf(quality);
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
 
        if (_format.equals(DocumentFormat.BINARY)) {
            setFormat(DocumentFormat.BINARY);
            return;
        }

        // default
        setFormat(DocumentFormat.JSON);
    }

    /**
     * @param _capability
     * @param _role
     */
    public void addPermission(String _capability, String _role) {
        ContentCapability capability;
        if (ContentPermission.UPDATE.toString().equals(_capability)) {
            capability = ContentPermission.UPDATE;
        } else if (ContentPermission.INSERT.toString().equals(_capability)) {
            capability = ContentPermission.INSERT;
        } else if (ContentPermission.EXECUTE.toString().equals(_capability)) {
            capability = ContentPermission.EXECUTE;
        } else if (ContentPermission.READ.toString().equals(_capability)) {
            capability = ContentPermission.READ;
        } else {
            throw new UnimplementedFeatureException(
                    "unknown capability: " + _capability);
        }
        addPermission(new ContentPermission(capability, _role));
    }

    public String getPermString() {
        return permString;
    }

    public void setPermString(String permString) {
        this.permString = permString;
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
    
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || !(obj instanceof DocumentMetadata)) {
            return false;
        }

        DocumentMetadata metadata = (DocumentMetadata) obj;
        boolean result = compareCollections(metadata.getCollections())
            && getFormatName().equals(metadata.getFormatName())
            && comparePermissions(metadata.getPermissions())
            && compareProperties(metadata.getProperties())
            && getQuality() == metadata.getQuality()
            && meta.equals(metadata.getMeta());

        return result;
    }
    
    private boolean compareProperties(String properties) {
        if (this.properties == properties) {
            return true;
        }
        if (this.properties != null) {
            return this.properties.equals(properties);
        }
        return false;
    }

    private boolean compareCollections(String[] cols) {
        if (cols == null || collectionsList.size() != cols.length) {
            return false;
        }
        for (int i = 0; i < collectionsList.size(); i++) {
            if (!collectionsList.get(i).equals(cols[i])) {
                return false;
            }
        }
        return true;
    }
    private boolean comparePermissions(ContentPermission[] p) {
        if(p == null && getPermissions() == null) {
            return true;
        }
        if (permissionsList.size() != p.length) {
            return false;
        }
        for (int i = 0; i < permissionsList.size(); i++) {
            if (!permissionsList.get(i).toString().equals(p[i].toString())) {
                return false;
            }
        }
        return true;
    }
}
