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
 * Document URI used as a key for a document record.
 * 
 * @author jchen
 */
public class DocumentURI implements WritableComparable<DocumentURI> {
	// TODO: revisit -- is it faster to use Text or String?
	private String uri;

	public DocumentURI() {}
	
	public DocumentURI(String uri) {
		this.uri = uri;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		uri = Text.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, uri);
	}

	public String getUri() {
		return uri;
	}

	@Override
	public int compareTo(DocumentURI o) {
		return uri.compareTo(o.getUri());
	}
}
