/*
 * Copyright 2003-2014 MarkLogic Corporation
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

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

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
    private static TransformerFactory transformerFactory = null;

    private static synchronized TransformerFactory getTransformerFactory() {
        if (transformerFactory == null) {
            transformerFactory = TransformerFactory.newInstance();
        }

        return transformerFactory;
    }
    
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
    
    static ByteArrayOutputStream serialize(Node node) 
    throws TransformerException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Result rslt = new StreamResult(bos);
        Source src = new DOMSource(node);
        Transformer transformer = getTransformerFactory().newTransformer();
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, 
                "yes");
        transformer.transform(src, rslt);

        return bos;
    }

    public String toString() {
        if (rootNodeKind == NodeKind.TEXT) {
            TextImpl textNode = (TextImpl) doc.getFirstChild();
            if (textNode != null) {
                return textNode.getTextContent();
            }    
        }
        try {
            ByteArrayOutputStream bos = serialize(doc);
            return bos.toString();
        } catch (TransformerException ex) {
            LOG.error("Error serializing document", ex);
        }    
        return null;
    }

    @Override
    public byte[] getContentAsByteArray() {
        if (rootNodeKind == NodeKind.TEXT) {
            TextImpl textNode = (TextImpl) doc.getFirstChild();
            if (textNode != null) {
                return textNode.getTextContent().getBytes();
            }          
        }
        try {
            ByteArrayOutputStream bos = serialize(doc);
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
        if (rootNodeKind == NodeKind.ELEM ||
            rootNodeKind == NodeKind.COMMENT ||
            rootNodeKind == NodeKind.PI) {
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
