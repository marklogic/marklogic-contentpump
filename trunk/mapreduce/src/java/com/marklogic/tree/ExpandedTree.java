package com.marklogic.tree;

import java.nio.charset.Charset;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.w3c.dom.Node;

import com.marklogic.dom.AttrImpl;
import com.marklogic.dom.CommentImpl;
import com.marklogic.dom.DocumentImpl;
import com.marklogic.dom.ElementImpl;
import com.marklogic.dom.NodeImpl;
import com.marklogic.dom.ProcessingInstructionImpl;
import com.marklogic.dom.TextImpl;

public class ExpandedTree {

	private static final Charset UTF8 = Charset.forName("UTF8");
// TODO: check data type correctness: byte for char, int for unsigned
	NodeImpl nodes[]; // NodeRep*
	
	public long ordinal;  // uint64_t
	public long uriKey;   // uint64_t
	public long uniqKey;  // uint64_t
	public long linkKey;  // uint64_t
	public long keys[];   // uint64_t*
	public byte atomData[]; // char*
	public String atomString[];
	public int atomIndex[]; // unsigned*
//	uint64_t* atomHashes;
//	uint64_t* atomLCHashes;
//	uint64_t* atomDLHashes;
//	uint64_t* atomLCDLHashes;
	
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
//	uint64_t languageHash; // probably not used
	protected long fragmentOrdinal;

	public boolean atomEquals(int atom, byte value[]) {
		int p = 0;
		int i = atomIndex[atom] + 1;
		while (p < value.length) {
			byte b = atomData[i];
			// System.out.println(String.format("%02x %02x", b, value[p]));
			if ((b == 0) || (b != value[p]))
				return false;
			p++;
			i++;
		}
		return true;
	}

	public String atomString(int i) {
		// TODO: memorize only node names
		String value = null;
		if (atomString == null) {
			atomString = new String[atomIndex.length];
		} else {
			value = atomString[i];
		}
		if (value == null) {
			// TODO: why all the +1 / -2 here?  not quite the same in C++
			value = atomString[i] = new String(atomData, atomIndex[i] + 1,
					atomIndex[i + 1] - atomIndex[i] - 2, UTF8);
		}
		return value;
	}
	
	public String getText(int index) {
    	// TODO: keep for reuse (?)
		// TODO: count bytes, build once rather than calling atomString
	    if (textReps==null) return null;
    	StringBuilder buf = new StringBuilder();
    	for (int i=textReps[index++]; i > 0; --i) {
//    		System.out.println("atom " + textReps[index] + " [" + atomString(textReps[index]) + "] length " + atomString(textReps[index]).length());
    		buf.append(atomString(textReps[index++]));
    	}
//    	System.out.println("getText(" + index + ") returning [" + buf.toString() + "] length " + buf.length());
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

//	public Node rootNode() {
//		return node(0);
//	}
	
	public byte rootNodeKind() {
	    if (node(0) != null) {
	        return nodeKind[((DocumentImpl)node(0)).getFirstChildIndex()];
	    } else return nodeKind[0];
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
				System.out.println("No support for link node.");
				break;
			case NodeKind.NS:
				System.out.println("No support for namespace node.");
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
				System.out.println("Skipping permission node.");
				break;
			case NodeKind.BINARY:
			    if (binaryData == null) { // large binary	        
			        System.out.println("large binary. binary size = " + 
			                binarySize + ", binary path = " +
			                getPathToBinary());
			    } else {
			        System.out.println("binary length = " + binaryData.length);
			    }				
				break;
			default:
				System.out.println("Unable to create node kind " + nodeKind[i] + " @ " + i);
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
}
