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
package com.marklogic.tree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.w3c.dom.Node;

import com.marklogic.dom.AttrImpl;
import com.marklogic.dom.CommentImpl;
import com.marklogic.dom.DocumentImpl;
import com.marklogic.dom.ElementImpl;
import com.marklogic.dom.NodeImpl;
import com.marklogic.dom.ProcessingInstructionImpl;
import com.marklogic.dom.TextImpl;

/**
 * Java equivalent of ExpandedTreeRep in Tree.h
 * 
 * @author jchen
 */
public class ExpandedTree implements Writable {
    public static final Log LOG = LogFactory.getLog(ExpandedTree.class);
	private static final Charset UTF8 = Charset.forName("UTF8");

	NodeImpl nodes[]; // NodeRep*
	
	public long ordinal;  // uint64_t
	public long uriKey;   // uint64_t
	public long uniqKey;  // uint64_t
	public long linkKey;  // uint64_t
	public long keys[];   // uint64_t*
	public byte atomData[]; // char*
	public String atomString[];
	public int atomIndex[]; // unsigned*
	
	public long nodeOrdinal[];
	public byte nodeKind[];
	public int nodeRepID[];
	public int nodeParentNodeRepID[];

	public int docNodeTextRepID[]; // unsigned DocNodeRep::textRepID
	public int docNodeChildNodeRepID[]; // unsigned DocNodeRep::childNodeRepID
	public int docNodeNumChildren[]; // unsigned DocNodeRep::numChildren

	public int elemNodeNodeNameRepID[]; // unsigned ElemNodeRep::nodeNameRepID
	public int elemNodeAttrNodeRepID[]; // unsigned ElemNodeRep::attrNodeRepID
	public int elemNodeChildNodeRepID[]; // unsigned ElemNodeRep::childNodeRepID
	public int elemNodeElemDeclRepID[]; // unsigned ElemNodeRep::elemDeclRepID
	public int elemNodeNumAttributes[]; // unsigned ElemNodeRep::numAttributes:24
	public int elemNodeNumDefaultAttrs[]; // unsigned ElemNodeRep::numDefaultAttrs:8
	public int elemNodeNumChildren[]; // unsigned ElemNodeRep::numChildren:28
	public int elemNodeFlags[]; // unsigned ElemNodeRep::flags

	public int attrNodeNodeNameRepID[];
	public int attrNodeTextRepID[];
	public int attrNodeAttrDeclRepID[];

	public int piNodeTargetAtom[];
	public int piNodeTextRepID[];

	public long linkNodeKey[];
	public long linkNodeNodeCount[];
	public int linkNodeNodeNameRepID[];
	public int linkNodeNodeRepID[];

	public int nodeNameNameAtom[];
	public int nodeNameNamespaceAtom[];

	public long nsNodeOrdinal[];
	public int nsNodePrevNSNodeRepID[];
	public int nsNodePrefixAtom[];
	public int nsNodeUriAtom[];

	public long permNodeOrdinal[];
	public int permNodePrevPermNodeRepID[];
	public Capability permNodeCapability[];
	public long permNodeRoleId[];
	
	public long binaryKey; // uint64_t BinaryNodeRep.binaryKey
	public long binaryOffset; // uint64_t BinaryNodeRep.offset
	public long binarySize; // uint64_t BinaryNodeRep.size
	public long binaryOrigLen; // uint64_t BinaryNodeRep.originalLength
	public int binaryPathAtom; // unsigned BinaryNodeRep.pathAtom

	public int numTextReps; // not in ExpandedTreeRep
	public int textReps[]; // unsigned*
	public int binaryData[]; // unsigned*
	
	public int atomLimit;  // unsigned
	public int numKeys;  // unsigned
	public int numNodeReps; // unsigned
	public int numNSNodeReps; // unsigned
	public int numPermNodeReps; // unsigned
	public int numLinkNodeReps; // unsigned
	public int uriTextRepID; // unsigned
	public int colsTextRepID; // unsigned
	public int schemaRepUID; // unsigned
	public long schemaTimestamp; // uint64_t

	private long fragmentOrdinal;

	public boolean atomEquals(int atom, byte value[]) {
		int p = 0;
		int i = atomIndex[atom] + 1;
		while (p < value.length) {
			byte b = atomData[i];
			if (LOG.isTraceEnabled()) {
			    LOG.trace(String.format("%02x %02x", b, value[p]));
			}
			if ((b == 0) || (b != value[p]))
				return false;
			p++;
			i++;
		}
		return true;
	}
	
	public String atomString(int i) {
		String value = null;
		if (atomString == null) {
			atomString = new String[atomIndex.length];
		} else if (atomString.length > i){
			value = atomString[i];
		}
		if (value == null) {
			value = atomString[i] = new String(atomData, atomIndex[i] + 1,
					atomIndex[i + 1] - atomIndex[i] - 2, UTF8);
		}
		return value;
	}
	
	public String getText(int index) {
	    if (textReps==null) return null;
    	StringBuilder buf = new StringBuilder();
    	for (int i=textReps[index++]; i > 0; --i) {
    	    if (LOG.isTraceEnabled()) {
    	        LOG.trace("atom " + textReps[index] + " [" + 
    	            atomString(textReps[index]) + "] length " + 
    	            atomString(textReps[index]).length());
    	    }
    		buf.append(atomString(textReps[index++]));
    	}
        if (LOG.isTraceEnabled()) {
            LOG.trace("getText(" + index + ") returning [" + buf.toString() + 
                    "] length " + buf.length());
        }
        return buf.toString();
	}
	
    public String[] getCollections() {
        int index = colsTextRepID;
        int cnt = textReps[index++];
        String[] cols = new String[cnt];
        for (int i = 0; i < cnt; ++i) {
            cols[i] = atomString(textReps[index++]);
        }
        return cols;
    }
	
	public byte rootNodeKind() {
	    if (node(0) != null) {
	        return nodeKind[((DocumentImpl)node(0)).getFirstChildIndex()];
	    } else {
	        return nodeKind[0];
	    }
	}
	
	public Node node(int i) {
		if (i == Integer.MAX_VALUE) {
			return null;
		}
		else if (nodes[i] != null) {
			return nodes[i];
		}
		else {
			switch (nodeKind[i]) {
			case NodeKind.ELEM:
				nodes[i] = new ElementImpl(this, i);
				break;
            case NodeKind.ATTR:
                nodes[i] = new AttrImpl(this, i);
                break;
			case NodeKind.TEXT:
				nodes[i] = new TextImpl(this, i);
				break;
			case NodeKind.LINK:
				break;
			case NodeKind.NS:
				break;
            case NodeKind.DOC:
                nodes[i] = new DocumentImpl(this, i);
                break;
			case NodeKind.PI:
				nodes[i] = new ProcessingInstructionImpl(this, i);
				break;
			case NodeKind.COMMENT:
				nodes[i] = new CommentImpl(this, i);
				break;
			case NodeKind.PERM:
				break;
			case NodeKind.BINARY:		
				break;
			default:
				LOG.warn("Unexpected node kind: " + nodeKind[i] + " @ " + i);
				break;
			}
			return nodes[i];
		}
	}

    public String getDocumentURI() {
        return getText(uriTextRepID);
    }
    
    public Path getPathToBinary() {
        long dirKey = binaryKey >>> 54;
        String dir = String.format("%03x", dirKey);
        String fileName = String.format("%16x", binaryKey);
        return new Path(dir, fileName);
    }

    public boolean containLinks() {
        return numLinkNodeReps > 0;
    }

    public long getFragmentOrdinal() {
        return fragmentOrdinal;
    }

    public void setFragmentOrdinal(long fragmentOrdinal) {
        this.fragmentOrdinal = fragmentOrdinal;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        uriKey = in.readLong();
        uniqKey = in.readLong();
        linkKey = in.readLong();
        numKeys = in.readInt();
        if (numKeys > 0) {
            keys = new long[numKeys];
            for (int i = 0; i < numKeys; i++) {
                keys[i] = in.readLong();
            }
        }
        int atomDataLen = in.readInt();
        if (atomDataLen > 0) {
            atomData = new byte[atomDataLen];
            for (int i = 0; i < atomDataLen; i++) {
                atomData[i] = in.readByte();
            }
        }
        atomLimit = in.readInt();
        if (atomLimit > 0) {
            atomIndex = new int[atomLimit+1];
            for (int i = 0; i < atomLimit + 1; i++) {
                atomIndex[i] = in.readInt();               
            }
        }
        int nodeNameNameAtomLen = in.readInt();
        if (nodeNameNameAtomLen > 0) {
            nodeNameNameAtom = new int[nodeNameNameAtomLen];
            nodeNameNamespaceAtom = new int[nodeNameNameAtomLen];
            for (int i = 0; i < nodeNameNameAtomLen; i++) {
                nodeNameNameAtom[i] = in.readInt();
                nodeNameNamespaceAtom[i] = in.readInt();
            }
        }
        numNodeReps = in.readInt();
        if (numNodeReps > 0) {
            nodes = new NodeImpl[numNodeReps]; 
            nodeOrdinal = new long[numNodeReps];
            nodeKind = new byte[numNodeReps];
            nodeRepID = new int[numNodeReps];
            nodeParentNodeRepID = new int[numNodeReps];
            for (int i = 0; i < numNodeReps; i++) {
                nodeOrdinal[i] = in.readLong();
                nodeKind[i] = in.readByte();
                nodeRepID[i] = in.readInt();
                nodeParentNodeRepID[i] = in.readInt();
            }
        }
        int numElemNodeReps = in.readInt();
        if (numElemNodeReps > 0) {
            elemNodeNodeNameRepID = new int [numElemNodeReps];
            elemNodeAttrNodeRepID = new int[numElemNodeReps];
            elemNodeChildNodeRepID = new int[numElemNodeReps];
            elemNodeElemDeclRepID = new int[numElemNodeReps];
            elemNodeNumAttributes = new int[numElemNodeReps];
            elemNodeNumDefaultAttrs = new int[numElemNodeReps];
            elemNodeNumChildren = new int[numElemNodeReps];
            elemNodeFlags = new int[numElemNodeReps];
            for (int i = 0; i < numElemNodeReps; i++) {
                elemNodeNodeNameRepID[i] = in.readInt();
                elemNodeAttrNodeRepID[i] = in.readInt();
                elemNodeChildNodeRepID[i] = in.readInt();
                elemNodeElemDeclRepID[i] = in.readInt();
                elemNodeNumAttributes[i] = in.readInt();
                elemNodeNumDefaultAttrs[i] = in.readInt();
                elemNodeNumChildren[i] = in.readInt();
                elemNodeFlags[i] = in.readInt();
            }
        }
        int numAttrNodeReps = in.readInt();
        if (numAttrNodeReps > 0) {
            attrNodeNodeNameRepID = new int[numAttrNodeReps];
            attrNodeTextRepID = new int[numAttrNodeReps];
            attrNodeAttrDeclRepID = new int[numAttrNodeReps];
            for (int i = 0; i < numAttrNodeReps; i++) {
                attrNodeNodeNameRepID[i] = in.readInt();
                attrNodeTextRepID[i] = in.readInt();
                attrNodeAttrDeclRepID[i] = in.readInt();
            }
        }
        numLinkNodeReps = in.readInt();
        if (numLinkNodeReps > 0) {
            linkNodeKey = new long[numLinkNodeReps];
            linkNodeNodeCount = new long[numLinkNodeReps];
            linkNodeNodeNameRepID = new int[numLinkNodeReps];
            linkNodeNodeRepID = new int[numLinkNodeReps];
            for (int i = 0; i < numLinkNodeReps; i++) {
                linkNodeKey[i] = in.readLong();
                linkNodeNodeCount[i] = in.readLong();
                linkNodeNodeNameRepID[i] = in.readInt();
                linkNodeNodeRepID[i] = in.readInt();
            }
        }
        int numDocNodeReps = in.readInt();
        if (numDocNodeReps > 0) {
            docNodeTextRepID = new int[numDocNodeReps];
            docNodeChildNodeRepID = new int[numDocNodeReps];
            docNodeNumChildren = new int[numDocNodeReps];
            for (int i = 0; i < numDocNodeReps; i++) {
                docNodeTextRepID[i] = in.readInt();
                docNodeChildNodeRepID[i] = in.readInt();
                docNodeNumChildren[i] = in.readInt();
            }
        }
        int numPINodeReps = in.readInt();
        if (numPINodeReps > 0) {
            piNodeTargetAtom = new int[numPINodeReps];
            piNodeTextRepID = new int[numPINodeReps];
            for (int i = 0; i < numPINodeReps; i++) {
                piNodeTargetAtom[i] = in.readInt();
                piNodeTextRepID[i] = in.readInt();
            }
        }
        numNSNodeReps = in.readInt();
        if (numNSNodeReps > 0) {
            nsNodeOrdinal = new long[numNSNodeReps];
            nsNodePrevNSNodeRepID = new int[numNSNodeReps];
            nsNodePrefixAtom = new int[numNSNodeReps];
            nsNodeUriAtom = new int[numNSNodeReps];
            for (int i = 0; i < numNSNodeReps; i++) {
                nsNodeOrdinal[i] = in.readLong();
                nsNodePrevNSNodeRepID[i] = in.readInt();
                nsNodePrefixAtom[i] = in.readInt();
                nsNodeUriAtom[i] = in.readInt();
            }
        }
        // skip permission node since it's not exposed to the API
        uriTextRepID = in.readInt();
        colsTextRepID = in.readInt();
        numTextReps = in.readInt();
        if (numTextReps > 0) {
            textReps = new int[numTextReps];
            for (int i = 0; i < numTextReps; i++) {
                textReps[i] = in.readInt();
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(uriKey);
        out.writeLong(uniqKey);
        out.writeLong(linkKey);
        out.writeInt(numKeys);
        if (numKeys > 0) {
           for (long key : keys) {
               out.writeLong(key);
           }
        }
        if (atomData != null && atomData.length > 0) {
            out.writeInt(atomData.length);
            for (int i = 0; i < atomData.length; i++) {
                out.writeByte(atomData[i]);
            }
        } else {
            out.writeInt(0);
        }
        out.writeInt(atomLimit);
        if (atomIndex != null && atomIndex.length > 0) {
            for (int i = 0; i < atomIndex.length; i++) {
                out.writeInt(atomIndex[i]);
            }
        }
        if (nodeNameNameAtom != null && nodeNameNameAtom.length > 0) {
            out.writeInt(nodeNameNameAtom.length);
            for (int i = 0; i < nodeNameNameAtom.length; i++) {
                out.writeInt(nodeNameNameAtom[i]);
                out.writeInt(nodeNameNamespaceAtom[i]);
            }
        } else {
            out.writeInt(0);
        }
        out.writeInt(numNodeReps);
        if (numNodeReps > 0) {
            for (int i = 0; i < numNodeReps; i++) {
                out.writeLong(nodeOrdinal[i]);
                out.writeByte(nodeKind[i]);
                out.writeInt(nodeRepID[i]);
                out.writeInt(nodeParentNodeRepID[i]);
            }
        }
        if (elemNodeNodeNameRepID != null && 
            elemNodeNodeNameRepID.length > 0) {
            out.writeInt(elemNodeNodeNameRepID.length);
            for (int i = 0; i < elemNodeNodeNameRepID.length; i++) {
                out.writeInt(elemNodeNodeNameRepID[i]);
                out.writeInt(elemNodeAttrNodeRepID[i]);
                out.writeInt(elemNodeChildNodeRepID[i]);
                out.writeInt(elemNodeElemDeclRepID[i]);
                out.writeInt(elemNodeNumAttributes[i]);
                out.writeInt(elemNodeNumDefaultAttrs[i]);
                out.writeInt(elemNodeNumChildren[i]);
                out.writeInt(elemNodeFlags[i]);
            }
        } else {
            out.writeInt(0);
        }
        if (attrNodeNodeNameRepID != null && 
            attrNodeNodeNameRepID.length > 0) {
            out.writeInt(attrNodeNodeNameRepID.length);
            for (int i = 0; i < attrNodeNodeNameRepID.length; i++) {
                out.writeInt(attrNodeNodeNameRepID[i]);
                out.writeInt(attrNodeTextRepID[i]);
                out.writeInt(attrNodeAttrDeclRepID[i]);
            }
        } else {
            out.writeInt(0);
        }
        out.writeInt(numLinkNodeReps);
        if (numLinkNodeReps > 0) {
            for (int i = 0; i < numLinkNodeReps; i++) {
                out.writeLong(linkNodeKey[i]);
                out.writeLong(linkNodeNodeCount[i]);
                out.writeInt(linkNodeNodeNameRepID[i]);
                out.writeInt(linkNodeNodeRepID[i]);
            }
        }
        if (docNodeTextRepID != null && docNodeTextRepID.length > 0) {
            out.writeInt(docNodeTextRepID.length);
            for (int i = 0; i < docNodeTextRepID.length; i++) {
                out.writeInt(docNodeTextRepID[i]);
                out.writeInt(docNodeChildNodeRepID[i]);
                out.writeInt(docNodeNumChildren[i]);
            }
        } else {
            out.writeInt(0);
        }
        if (piNodeTargetAtom != null && piNodeTargetAtom.length > 0) {
            out.writeInt(piNodeTargetAtom.length);
            for (int i = 0; i < piNodeTargetAtom.length; i++) {
                out.writeInt(piNodeTargetAtom[i]);
                out.writeInt(piNodeTextRepID[i]);
            }
        } else {
            out.writeInt(0);
        }
        out.writeInt(numNSNodeReps);
        if (numNSNodeReps > 0) {
            for (int i = 0; i < numNSNodeReps; i++) {
                out.writeLong(nsNodeOrdinal[i]);
                out.writeInt(nsNodePrevNSNodeRepID[i]);
                out.writeInt(nsNodePrefixAtom[i]);
                out.writeInt(nsNodeUriAtom[i]);
            }
        }
        // skip permission node since it's not exposed to the API
        out.writeInt(uriTextRepID);
        out.writeInt(colsTextRepID);
        out.writeInt(numTextReps);
        if (numTextReps > 0) {
            for (int i = 0; i < numTextReps; i++) {
                out.writeInt(textReps[i]);
            }
        }
    }    
}
