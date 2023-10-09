/*
 * Copyright (c) 2021 MarkLogic Corporation
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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;

import com.marklogic.tree.ExpandedTree;
import com.marklogic.tree.NodeKind;
import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;

/**
 * A {@link MarkLogicDocument} retrieved from a MarkLogic forest through 
 * Direct Access.
 * 
 * @see ForestInputFormat
 * @author jchen
 */
public abstract class ForestDocument implements MarkLogicDocument {
    public static final Log LOG = LogFactory.getLog(
            ForestDocument.class);
    
    private long fragmentOrdinal;
    private String[] collections;
    private int quality;
    private Map<String, String> metadata = null;
    
    public static ForestDocument createDocument(Configuration conf,
            Path forestDir, ExpandedTree tree, String uri) {
        byte rootNodeKind = tree.rootNodeKind();
        ForestDocument doc = null;
        switch (rootNodeKind) {
            case NodeKind.BINARY:
                if (tree.binaryData == null) {
                    doc = new LargeBinaryDocument(conf, forestDir, tree);
                } else {
                    doc = new RegularBinaryDocument(tree);
                }
                break;
            case NodeKind.ELEM:
            case NodeKind.TEXT:
            case NodeKind.PI:
            case NodeKind.COMMENT:
                doc = new DOMDocument(tree);  
                break;
            case NodeKind.ARRAY:
            case NodeKind.OBJECT:
            case NodeKind.NULL:
            case NodeKind.BOOLEAN:
            case NodeKind.NUMBER:
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Creating JSONDocument "
                            + rootNodeKind + " (" + uri + ")");
                }
                doc = new JSONDocument(tree);  
                break;
            default:
                return null;
        }
        doc.setFragmentOrdinal(tree.getFragmentOrdinal());
        doc.setCollections(tree.getDocumentURI(), 
                tree.getCollections());
        doc.setMetadata(tree.getMetadata());
        doc.setQuality(tree.getQuality());
        return doc;
    }

    public long getFragmentOrdinal() {
        return fragmentOrdinal;
    }
    
    private void setFragmentOrdinal(long fragOrdinal) {
        fragmentOrdinal = fragOrdinal; 
    }
    
    public String[] getCollections() {
        return collections;
    }
    
    private void setCollections(String docURI, String[] cols) {
        // filter out collections that are illegal
        List<String> colList = new ArrayList<>(cols.length);
        for (String col : cols) {
            if (col.isEmpty()) { // Java URI does allow empty string.
                LOG.info("Empty collection URI is removed for document "
                        + docURI);
                continue;
            }
            // We could drop other illegal URIs, but server seems to be more 
            // lenient than Java URI, so try to preserve the original 
            // collections.
            colList.add(col); 
        }
        collections = new String[colList.size()];
        colList.toArray(collections);
    }
    
    public Map<String, String> getMetadata() {
        return metadata;
    }

    private void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        fragmentOrdinal = in.readLong();
        collections = WritableUtils.readStringArray(in);
        int numMetadata = in.readInt();
        if (numMetadata > 0) {
            String[] metaStrings = WritableUtils.readStringArray(in);
            metadata = new HashMap<>(numMetadata);
            for (int i = 0; i < metaStrings.length - 1; i++) {
                metadata.put(metaStrings[i], metaStrings[i + 1]);
            }
        }
        quality = in.readInt();
    }

    public int getQuality() {
        return quality;
    }

    public void setQuality(int quality) {
        this.quality = quality;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(fragmentOrdinal);
        WritableUtils.writeStringArray(out, collections);
        if (metadata == null || metadata.isEmpty()) {
            out.writeInt(0);
        } else {
            out.writeInt(metadata.size());
            String[] metaStrings = new String[metadata.size() * 2];
            int i = 0;
            for (Entry<String, String> entry : metadata.entrySet()) {
                metaStrings[i++] = entry.getKey();
                metaStrings[i++] = entry.getValue();
            }
            WritableUtils.writeStringArray(out, metaStrings);
        }
        out.writeInt(quality);
    }
    
    @Override
    public InputStream getContentAsByteStream() {
        return new ByteArrayInputStream(getContentAsByteArray());
    }
    
    @Override
    public long getContentSize() {
        byte[] buf = getContentAsByteArray();
        return buf.length;
    }

    @Override
    public boolean isStreamable() {
        return false;
    }
    
    abstract public Content createContent(String uri, 
            ContentCreateOptions options, boolean copyCollections, 
            boolean copyMetadata, boolean copyQuality) 
    throws IOException;
    
    protected void setContentOptions(ContentCreateOptions options,
            boolean copyCollections, boolean copyMetadata, 
            boolean copyQuality) {
        if (copyCollections && collections.length != 0) {
            String[] cols = options.getCollections();
            if (cols == null || cols.length == 0) {
                options.setCollections(collections);
            } else { // merge
                HashSet<String> colsSet = new HashSet<>();
                if (cols != null) {
                    for (String col : cols) {
                        colsSet.add(col);
                    }
                }
                for (String col : collections) {
                    colsSet.add(col);
                }
                String[] newCols = new String[colsSet.size()];
                colsSet.toArray(newCols);
                options.setCollections(newCols);
            }
        }
        if (copyMetadata) {
            options.setMetadata(metadata);
        }
        if (copyQuality) {
            options.setQuality(quality);
        }
    }
}
