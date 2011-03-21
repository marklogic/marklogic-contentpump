/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
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
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.modeler.util.DomUtil;
import org.apache.hadoop.io.Writable;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.types.ItemType;
import com.marklogic.xcc.types.XdmBinary;
import com.marklogic.xcc.types.XdmElement;
import com.marklogic.xcc.types.XdmText;

/**
 * A record returned by MarkLogic, used to represent an XML node.
 * 
 * @author jchen
 */
public class MarkLogicNode implements Writable {
	public static final Log LOG =
	    LogFactory.getLog(MarkLogicNode.class);
	
	private Node node;
	
	public MarkLogicNode() {}
	
	public MarkLogicNode(Node node) {
		this.node = node;
	}
	
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
	
	public MarkLogicNode(ResultItem item) {
		ItemType type =	item.getItemType();
		DocumentBuilder docBuilder = builderLocal.get();
	    try {
			if (type == ItemType.BINARY) {
				node = ((XdmBinary)item.getItem()).asW3cNode(docBuilder);
			} else if (type == ItemType.ELEMENT) {
				node = ((XdmElement)item.getItem()).asW3cElement(docBuilder);
			} else if (type == ItemType.TEXT) {
			    node = ((XdmText)item.getItem()).asW3cText(docBuilder);
			} else {
				LOG.error("Unexpected item type: " + item.getItemType());
			}
		} catch (IOException e) {
			LOG.error(e);
		} catch (SAXException e) {
			LOG.error(e);
		}
	}
	
	public Node getNode() {
		return node;
	}
	
	public void setNode(Node node) {
		this.node = node;
	}
	
	public void readFields(DataInput in) throws IOException {
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

	public void write(DataOutput out) throws IOException {
		if (node != null) {
			try {
				DomUtil.writeXml(node, (DataOutputStream)out);
			} catch (TransformerException e) {
				LOG.error(e);
			}
		} else {
		    LOG.error("Node to write is null.");
		}
	}
	
	@Override
	public String toString() {
		if (node != null) {
			try {
				ByteArrayOutputStream bos = new ByteArrayOutputStream();
				TransformerFactory tf = TransformerFactory.newInstance();
				Transformer t = tf.newTransformer();
				t.setOutputProperty(OutputKeys.INDENT, "yes");
                t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                t.transform(new DOMSource(node), new StreamResult(bos));
				return bos.toString();
			} catch (TransformerException e) {
				LOG.error(e);
			}
		}
		return null;
	}

}
