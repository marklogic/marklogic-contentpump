/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Node path as a key for a node record.
 * 
 * @author jchen
 */
public class NodePath implements WritableComparable<NodePath> {
    // TODO: revisit -- is it faster to use Text or String?
	private String docUri;

	private String path; // relative path
	
	public NodePath() {}
	
	public NodePath(String uri, String path) {
		docUri = uri;
		this.path = path;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		docUri = Text.readString(in);
        path = Text.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, docUri);
        Text.writeString(out, path);
	}

	public String getDocumentUri() {
		return docUri;
	}
	
	public void setDocumentUri(String docUri) {
		this.docUri = docUri;
	}
	
	public String getRelativePath() {
		return path;
	}
	
	public void setRelativePath(String path) {
		this.path = path;
	}
	
	public String getFullPath() {
	    StringBuilder buf = new StringBuilder();
	    buf.append("fn:doc(\"");
	    buf.append(DocumentURI.unparse(docUri));
	    buf.append("\")");
	    buf.append(path);
		return buf.toString();
	}

	@Override
	public int compareTo(NodePath o) {
		return (docUri + path).compareTo(o.getDocumentUri() + o.getRelativePath());
	}
}
