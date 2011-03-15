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
	    buf.append(unparse(docUri));
	    buf.append("\")");
	    buf.append(path);
		return buf.toString();
	}

	@Override
	public int compareTo(NodePath o) {
		return (docUri + path).compareTo(o.getDocumentUri() + o.getRelativePath());
	}
	
	private String unparse(String s) {
        int len = s.length();
        StringBuilder buf = new StringBuilder(len * 2);
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            if ((c >= 0x20) && (c < 0x80)) switch (c) {
            case '"':
                buf.append("&quot;");
                break;
            case '&':
                buf.append("&amp;");
                break;
            default:
                buf.append(c);
                break;
            }
            else {
                buf.append("&#x");
                buf.append(Integer.toString(c, 16));
                buf.append(';');
            }
        }
        return buf.toString();
    }

}
