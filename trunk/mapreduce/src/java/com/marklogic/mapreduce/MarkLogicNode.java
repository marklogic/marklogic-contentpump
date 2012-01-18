/*
 * Copyright (c) 2003-2012 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.modeler.util.DomUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.w3c.dom.Attr;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.types.ItemType;
import com.marklogic.xcc.types.XdmAttribute;
import com.marklogic.xcc.types.XdmDocument;
import com.marklogic.xcc.types.XdmElement;
import com.marklogic.xcc.types.XdmText;
import com.marklogic.xcc.types.impl.AttributeImpl;

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
        if (type == Node.ATTRIBUTE_NODE) {
            String name = Text.readString(in);
            String value = Text.readString(in);
            try {
                node = (new AttributeImpl(name, value)).asW3cAttr();
            } catch (ParserConfigurationException e) {
                LOG.error(e);
            } catch (SAXException e) {
                LOG.error(e);
            }
        } else {
            try {
                node = DomUtil.readXml((DataInputStream)in);
            } catch (ParserConfigurationException e) {
                LOG.error(e);
                throw new IOException(e);
            } catch (SAXException e) {
                LOG.error(e);
                throw new IOException(e);
            }
        }
    }

    public void write(DataOutput out) throws IOException {
        if (node != null) {
            short type = node.getNodeType();
            IntWritable typeWritable = new IntWritable(type);
            typeWritable.write(out);
            if (type == Node.ATTRIBUTE_NODE) {
                Attr attr = (Attr)node;
                Text.writeString(out, attr.getName());
                Text.writeString(out, attr.getValue());
            } else {       
                try {
                    DomUtil.writeXml(node, (DataOutputStream)out);
                } catch (TransformerException e) {
                    LOG.error(e);
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
