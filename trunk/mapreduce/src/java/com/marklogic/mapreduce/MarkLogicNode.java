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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.modeler.util.DomUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.w3c.dom.Attr;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import com.marklogic.io.IOHelper;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.types.ItemType;
import com.marklogic.xcc.types.XdmAttribute;
import com.marklogic.xcc.types.XdmDocument;
import com.marklogic.xcc.types.XdmElement;
import com.marklogic.xcc.types.XdmText;
import com.marklogic.xcc.types.impl.AttributeImpl;
import com.marklogic.xcc.types.impl.DocumentImpl;
import com.marklogic.xcc.types.impl.TextImpl;

/**
 * A record returned by MarkLogic, used to represent an XML node.  Currently 
 * only documents, elements and attributes are supported.  Use with
 * {@link NodeInputFormat} and {@link NodeOutputFormat}. May also be used
 * with {@link KeyValueInputFormat} and {@link ValueInputFormat}.
 * 
 * @author jchen
 */
public class MarkLogicNode implements Writable {
    public static final Log LOG =
        LogFactory.getLog(MarkLogicNode.class);
    
    private Node node;
    
    private static final ThreadLocal<DocumentBuilder> builderLocal = 
        new ThreadLocal<DocumentBuilder>() {         
        @Override protected DocumentBuilder initialValue() {
            try {
                return 
                DocumentBuilderFactory.newInstance().newDocumentBuilder();
            } catch (ParserConfigurationException e) {
                LOG.error(e);
                return null;
            } 
        }     
    }; 
    
    static DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();   
    
    public MarkLogicNode() {}
    
    public MarkLogicNode(Node node) {
        this.node = node;
    }
    
    public MarkLogicNode(ResultItem item) {
        set(item);
    }
    
    public MarkLogicNode(String content, ContentType contentType) {
        try {
            if (contentType == ContentType.TEXT) {
                node = new TextImpl(content).asW3cText();
            } else if (contentType == ContentType.XML) {
                node = new DocumentImpl(content).asW3cDocument();
            }
        } catch (IOException e) {
            LOG.error(e);
        } catch (SAXException e) {
            LOG.error("error parsing result", e);
        } catch (ParserConfigurationException e) {
            LOG.error(e);
        }
    }

    public Node get() {
        return node;
    }
    
    public void set(Node node) {
        this.node = node;
    }
    
    public void set(ResultItem item) {
        ItemType type = item.getItemType();
        DocumentBuilder docBuilder = builderLocal.get();
        try {
            if (type == ItemType.ELEMENT) {
                node = ((XdmElement)item.getItem()).asW3cElement(docBuilder);
            } else if (type == ItemType.TEXT) {
                node = ((XdmText)item.getItem()).asW3cText(docBuilder);
            } else if (type == ItemType.DOCUMENT) {
                node = ((XdmDocument)item.getItem()).asW3cDocument(docBuilder);
            } else if (type == ItemType.ATTRIBUTE) {
                node = ((XdmAttribute)item.getItem()).asW3cAttr(docBuilder);
            } else {
                throw new UnsupportedOperationException(
                        "Unexpected item type: " + item.getItemType());
            }
        } catch (IOException e) {
            LOG.error(e);
        } catch (SAXException e) {
            LOG.error("error parsing result", e);
        }
    }
    
    public void readFields(DataInput in) throws IOException {
        int type = in.readInt();
        DocumentBuilder docBuilder = builderLocal.get();
        String val = Text.readString(in);
        try {
            if (type == Node.ATTRIBUTE_NODE) {
                AttributeImpl attr = new AttributeImpl(Text.readString(in),
                        Text.readString(in));
                node = attr.asW3cNode(docBuilder);
            } else {
                node = DomUtil.readXml(IOHelper.newStream(val));
            } 
        } catch (SAXException e) {
            LOG.error("error parsing input", e);
            throw new IOException(e);
        } catch (ParserConfigurationException e) {
            LOG.error("error parsing input", e);
            throw new IOException(e);
        }   
    }

    public void write(DataOutput out) throws IOException {
        if (node != null) {
            int type = node.getNodeType();
            out.writeInt(type);
            if (type == Node.ATTRIBUTE_NODE) {
                Text.writeString(out, node.getNodeName());
                Text.writeString(out, node.getNodeValue());
            } else {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                try {
                    DomUtil.writeXml(node, baos);
                    Text.writeString(out, baos.toString());
                } catch (TransformerException e) {
                    LOG.error("error transforming node", e);
                    throw new IOException(e);
                }
            }
        } else {
            LOG.error("Node to write is null.");
        }
    }
 
    @Override
    public String toString() {
        if (node != null) {
            try {
                StringBuilder buf = new StringBuilder();
                if (node.getNodeType() == Node.ATTRIBUTE_NODE) {
                    buf.append("attribute name: ");
                    buf.append(((Attr)node).getName());
                    buf.append(", value: ");
                    buf.append(((Attr)node).getValue());
                } else {
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    DomUtil.writeXml(node, bos);
                    buf.append(bos.toString());
                }
                return buf.toString();
            } catch (TransformerException e) {
                LOG.error(e);
            }
        }
        return null;
    }
}
