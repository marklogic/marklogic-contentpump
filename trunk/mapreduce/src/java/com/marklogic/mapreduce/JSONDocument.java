/*
 * Copyright 2003-2015 MarkLogic Corporation
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
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

import com.marklogic.dom.NodeImpl;
import com.marklogic.dom.TextImpl;
import com.marklogic.tree.ExpandedTree;
import com.marklogic.tree.NodeKind;

/**
 * A {@link ForestDocument} JSON document node 
 * representation of a document as stored in the expanded tree
 * cache of a forest on disk.
 * 
 * <p>
 * You cannot use this class to modify a document. However, you
 * can create a modifiable copy of the underlying document
 * using {@link com.marklogic.dom.DocumentImpl} on the 
 * document returned by {@link #getDocument}.
 * </p>
 * 
 * @author jchen
 *
 */
public class JSONDocument extends ForestDocument {
    public static final Log LOG = LogFactory.getLog(JSONDocument.class);
    private static TransformerFactory transformerFactory = null;

    private static synchronized TransformerFactory getTransformerFactory() {
        if (transformerFactory == null) {
            transformerFactory = TransformerFactory.newInstance();
        }

        return transformerFactory;
    }

    private ExpandedTree tree;
    private byte rootNodeKind;
    private String str;

    public JSONDocument() {
    }
    
    public JSONDocument(ExpandedTree tree) {
        this.tree = tree;
        rootNodeKind = tree.rootNodeKind();
        str = tree.toString();
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        tree = new ExpandedTree();
        tree.readFields(in);
        rootNodeKind = tree.rootNodeKind();
        str = tree.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        tree.write(out);
    }
    
    public String toString() {
        return str;
    }

    @Override
    public byte[] getContentAsByteArray() {
        return str.getBytes();
    }

    @Override
    public Text getContentAsText() {
        return new Text(toString());
    }

    @Override
    public ContentType getContentType() {
        if (rootNodeKind == NodeKind.ARRAY ||
            rootNodeKind == NodeKind.OBJECT ||
            rootNodeKind == NodeKind.NULL ||
            rootNodeKind == NodeKind.BOOLEAN ||
            rootNodeKind == NodeKind.NUMBER) {
            return ContentType.JSON;
        } else if (rootNodeKind == NodeKind.TEXT) {
            return ContentType.TEXT;
        } else {
            throw new UnsupportedOperationException("Unknown node kind: " + 
                    rootNodeKind);
        }
    }

    @Override
    public MarkLogicNode getContentAsMarkLogicNode() {
        throw new UnsupportedOperationException("Unknown node kind: " +
            rootNodeKind);
    }

    @Override
    public String getContentAsString() throws UnsupportedEncodingException {
        return toString();
    }
}
