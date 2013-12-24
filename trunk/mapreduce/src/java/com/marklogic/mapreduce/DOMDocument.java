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

import com.marklogic.dom.NodeImpl;
import com.marklogic.dom.TextImpl;
import com.marklogic.tree.ExpandedTree;
import com.marklogic.tree.NodeKind;

/**
 * ForestDocument containing an expanded-tree-backed DOM document node.
 * 
 * @author jchen
 *
 */
public class DOMDocument extends ForestDocument {
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
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        ExpandedTree tree = new ExpandedTree();
        tree.readFields(in);
        doc = (Document)tree.node(0);
        rootNodeKind = tree.rootNodeKind();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        ((NodeImpl)doc).getExpandedTree().write(out);
    }

    public String toString() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        if (rootNodeKind == NodeKind.TEXT) {
            return ((TextImpl)doc).getTextContent();
        }
        try {
            DomUtil.writeXml(doc, bos);
            return bos.toString();
        } catch (TransformerException ex) {
            LOG.error("Error serializing document", ex);
        }    
        return null;
    }

    @Override
    public byte[] getContentAsByteArray() {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        if (rootNodeKind == NodeKind.TEXT) {
            return ((TextImpl)doc).getTextContent().getBytes();
        }
        try {
            DomUtil.writeXml(doc, bos);
            return bos.toByteArray();
        } catch (TransformerException ex) {
            LOG.error("Error serializing document", ex);
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
