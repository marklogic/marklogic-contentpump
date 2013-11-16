package com.marklogic.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import javax.xml.transform.TransformerException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.modeler.util.DomUtil;
import org.apache.hadoop.io.Text;
import org.w3c.dom.Document;

import com.marklogic.tree.ExpandedTree;
import com.marklogic.tree.NodeKind;

public class DOMDocument extends FileDocument {
    public static final Log LOG = LogFactory.getLog(DOMDocument.class);
    private Document doc;
    private byte rootNodeKind;
    
    public DOMDocument() {
    }
    
    public DOMDocument(ExpandedTree tree) {
        doc = (Document)tree.node(0);
        rootNodeKind = tree.rootNodeKind();
    }
    
    public Document getDocument() {
        return doc;
    }
    
    @Override
    public void readFields(DataInput arg0) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void write(DataOutput arg0) throws IOException {
        // TODO Auto-generated method stub

    }

    public String toString() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            DomUtil.writeXml(doc, bos);
            return bos.toString();
        } catch (TransformerException ex) {
            LOG.error(ex);
        }    
        return null;
    }

    @Override
    public byte[] getContentAsByteArray() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            DomUtil.writeXml(doc, bos);
            return bos.toByteArray();
        } catch (TransformerException ex) {
            LOG.error(ex);
        }    
        return null;
    }

    @Override
    public MarkLogicNode getContentAsMarkLogicNode() {
        return new MarkLogicNode(doc);
    }

    @Override
    public Text getContentAsText() {
        return new Text(toString());
    }

    @Override
    public ContentType getContentType() {
        if (rootNodeKind == NodeKind.ELEM) {
            return ContentType.XML;
        } else if (rootNodeKind == NodeKind.TEXT) {
            return ContentType.TEXT;
        } else {
            throw new UnsupportedOperationException("Unknown node kind: " + 
                    rootNodeKind);
        }
    }

    @Override
    public String getContentAsString() throws UnsupportedEncodingException {
        return toString();
    }
}
