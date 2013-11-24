package com.marklogic.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import com.marklogic.tree.ExpandedTree;
import com.marklogic.tree.NodeKind;

public abstract class ForestDocument implements MarkLogicDocument {
    public static final Log LOG = LogFactory.getLog(
            ForestDocument.class);
    
    private long fragmentOrdinal;
    private String[] collections;
    
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
                doc = new DOMDocument(tree);  
                break;
            default:
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Skipping unsupported node kind "
                            + rootNodeKind + " (" + uri + ")");
                }
                return null;
        }
        doc.setFragmentOrdinal(tree.getFragmentOrdinal());
        doc.setCollections(tree.getCollections());
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
    
    private void setCollections(String[] cols) {
        collections = cols;
    }
}
