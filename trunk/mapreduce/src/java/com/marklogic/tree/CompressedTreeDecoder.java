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
package com.marklogic.tree;

import java.io.DataInput;
import java.io.IOException;
import java.nio.charset.Charset;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.marklogic.dom.NodeImpl;
import com.marklogic.io.Decoder;

/**
 * Decoder of Compressed Tree.
 * 
 * @author jchen
 */
public class CompressedTreeDecoder {
    public static final Log LOG = LogFactory.getLog(
            CompressedTreeDecoder.class);
    private static final Charset UTF8 = Charset.forName("UTF8");

    private static final byte xmlURIBytes[] = 
            "http://www.w3.org/XML/1998/namespace".getBytes(UTF8);
    private static final byte xsiURIBytes[] = 
            "http://www.w3.org/2001/XMLSchema-instance".getBytes(UTF8);
    private static final byte spaceBytes[] = "space".getBytes(UTF8);
    private static final byte langBytes[] = "lang".getBytes(UTF8);
    private static final byte baseBytes[] = "base".getBytes(UTF8);
    private static final byte typeBytes[] = "type".getBytes(UTF8);

	static final int MAX_BINARY_BYTES = 512<<20; // 512 MB 

    private static final int xmlSpaceAttrPresentFlag = 0x01;
    private static final int xmlLangAttrPresentFlag = 0x02;
    private static final int xmlBaseAttrPresentFlag = 0x04;
    private static final int xsiTypeAttrPresentFlag = 0x08;

    public String utf8(String s) {
        byte b[] = s.getBytes(UTF8);
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < b.length; ++i) {
            buf.append(String.format("%02x", b[i] & 0xff));
        }
        return buf.toString();
    }

    private void decodeText(ExpandedTree rep, Decoder decoder, int atomLimit) 
    throws IOException {
        if (atomLimit == 0) return;
        int numAtoms = decoder.decodeUnsigned();
        int index = rep.numTextReps;
        int minSize = rep.numTextReps + numAtoms + 1;
        if (rep.textReps == null) {
            int size = Math.max(rep.atomLimit*16, minSize);
            rep.textReps = new int[size];
        } else if (rep.textReps.length < minSize) {
            int size = Math.max(rep.textReps.length*2, minSize);
            int textReps[] = new int[size];
            System.arraycopy(rep.textReps, 0, textReps, 0, index);
            rep.textReps = textReps;
        }
        rep.textReps[index++] = numAtoms;
        rep.numTextReps += numAtoms+1;
        for (int j = 0; j < numAtoms; j++) {
            int atom = decoder.decodeUnsigned();
            assert (atom < atomLimit);
            rep.textReps[index++] = atom;
        }
    }

    private void addText(ExpandedTree rep, int numKeys) 
    throws IOException {
        if (numKeys == 0) return;
        int index = rep.numTextReps;
        int minSize = rep.numTextReps + numKeys + 1;
        if (rep.textReps == null) {
            int size = Math.max(rep.atomLimit*16, minSize);
            rep.textReps = new int[size];
        } else if (rep.textReps.length < minSize) {
            int size = Math.max(rep.textReps.length*2, minSize);
            int textReps[] = new int[size];
            System.arraycopy(rep.textReps, 0, textReps, 0, index);
            rep.textReps = textReps;
        }
    }

    public ExpandedTree decode(DataInput in) throws IOException {
        String bad;
        Decoder decoder = new Decoder(in);
        ExpandedTree rep = new ExpandedTree();

        rep.uriKey = decoder.decode64bits();
        rep.uniqKey = decoder.decode64bits();
        rep.linkKey = decoder.decode64bits();

        rep.numKeys = decoder.decodeUnsigned();

        if (rep.numKeys == 0)
            rep.keys = null;
        else {
            rep.keys = new long[rep.numKeys];
            for (int i = 0; i < rep.numKeys; ++i) {
                rep.keys[i] = decoder.decode64bits();
            }
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("uriKey  %016x", rep.uriKey));
            LOG.trace(String.format("uniqKey %016x", rep.uniqKey));
            LOG.trace(String.format("linkKey %016x", rep.linkKey));
            for (int i = 0; i < rep.numKeys; ++i) {
                LOG.trace(String.format("  key[%d] %016x", i, rep.keys[i]));
            }
        }

        // atoms
        int numAtomDataWords = decoder.decodeUnsigned();
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("numAtomDataWords %d", numAtomDataWords));
        if (numAtomDataWords == 0)
            rep.atomData = null;
        else {
            rep.atomData = new byte[numAtomDataWords * 4];
            for (int i = 0, j = 0; i < numAtomDataWords; ++i) {
                int word = decoder.decode32bits();
                rep.atomData[j++] = (byte)(word & 0xff);
                rep.atomData[j++] = (byte)((word >> 8) & 0xff);
                rep.atomData[j++] = (byte)((word >> 16) & 0xff);
                rep.atomData[j++] = (byte)((word >> 24) & 0xff);
                if (LOG.isTraceEnabled()) {
                    LOG.trace(String.format("  atomData[%d] %08x", i, word));
                    LOG.trace(String.format(
                            "  atomData[%d] %02x %02x %02x %02x",
                            i, rep.atomData[i*4], rep.atomData[i*4+1],
                            rep.atomData[i*4+2], rep.atomData[i*4+3]));
                }
            }
        }
        rep.atomLimit = decoder.decodeUnsigned();
        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("atomLimit %d", rep.atomLimit));
        }

        if (rep.atomLimit == 0) {
            rep.atomIndex = null;
        } else {
            rep.atomIndex = new int[rep.atomLimit + 1];
            int j = 0;
            for (int i = 0; i < rep.atomLimit; ++i) {
                rep.atomIndex[i] = j;
                if (LOG.isTraceEnabled())
                    LOG.trace(String.format("  atomIndex[%d] %08x", i, 
                            rep.atomIndex[i]));
                if (rep.atomData != null) while (rep.atomData[j++] != 0);
            }
            rep.atomIndex[rep.atomLimit] = j;
        }
        for (int i = 0; i < rep.atomLimit; ++i) {
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("  atomString[%d] %s", i, 
                        rep.atomString(i)));
        }
        // node names
        int numNodeNameReps = decoder.decodeUnsigned();
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("numNodeNameReps %d", numNodeNameReps));
        if (numNodeNameReps == 0) {
            rep.nodeNameNameAtom = null;
            rep.nodeNameNamespaceAtom = null;
        } else {
            rep.nodeNameNameAtom = new int[numNodeNameReps];
            rep.nodeNameNamespaceAtom = new int[numNodeNameReps];
        }
        int xmlSpaceNodeNameRepID = Integer.MAX_VALUE;
        int xmlLangNodeNameRepID = Integer.MAX_VALUE;
        int xmlBaseNodeNameRepID = Integer.MAX_VALUE;
        int xsiTypeNodeNameRepID = Integer.MAX_VALUE;
        for (int j = 0; j < numNodeNameReps; j++) {
            rep.nodeNameNameAtom[j] = decoder.decodeUnsigned();
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("  nodeNameNameAtom[%d] %d", j, 
                        rep.nodeNameNameAtom[j]));
            assert (rep.nodeNameNameAtom[j] < rep.atomLimit);
            rep.nodeNameNamespaceAtom[j] = decoder.decodeUnsigned();
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("  nodeNameNamespaceAtom[%d] %d", j, 
                        rep.nodeNameNamespaceAtom[j]));
            assert (rep.nodeNameNamespaceAtom[j] < rep.atomLimit);
            if (rep.atomEquals(rep.nodeNameNamespaceAtom[j], xmlURIBytes)) {
                if (rep.atomEquals(rep.nodeNameNameAtom[j], spaceBytes))
                    xmlSpaceNodeNameRepID = j;
                else if (rep.atomEquals(rep.nodeNameNameAtom[j], langBytes)) {
                    xmlLangNodeNameRepID = j;
                } else if (rep.atomEquals(rep.nodeNameNameAtom[j], baseBytes))
                    xmlBaseNodeNameRepID = j;
            } else if (rep.atomEquals(rep.nodeNameNameAtom[j], xsiURIBytes)) {
                if (rep.atomEquals(rep.nodeNameNameAtom[j], typeBytes))
                    xsiTypeNodeNameRepID = j;
            }
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("xmlSpaceNodeNameRepID %d", 
                    xmlSpaceNodeNameRepID));
            LOG.trace(String.format("xmlLangNodeNameRepID %d", 
                    xmlLangNodeNameRepID));
            LOG.trace(String.format("xmlBaseNodeNameRepID %d", 
                    xmlBaseNodeNameRepID));
            LOG.trace(String.format("xsiTypeNodeNameRepID %d", 
                    xsiTypeNodeNameRepID));
        }
        int numElemNodeReps = 0;
        int numAttrNodeReps = 0;
        int numDocNodeReps = 0;
        int numPINodeReps = 0;
        int numArrayNodeReps = 0;
        int numDoubles = 0;
        // node counts
        rep.numNodeReps = decoder.decodeUnsigned();
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("numNodeReps %d", rep.numNodeReps));

        if (rep.numNodeReps==0) {
            // escape
            int version = decoder.decodeUnsigned();
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("version %d", version));
            assert(version==0);

            rep.numNodeReps = decoder.decodeUnsigned();
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("rep.numNodeReps %d", 
                    rep.numNodeReps));
            if (rep.numNodeReps > 0) {
                rep.nodes = new NodeImpl[rep.numNodeReps];
                rep.nodeOrdinal = new long[rep.numNodeReps];
                rep.nodeKind = new byte[rep.numNodeReps];
                rep.nodeRepID = new int[rep.numNodeReps];
                rep.nodeParentNodeRepID = new int[rep.numNodeReps];
            }
            numArrayNodeReps = decoder.decodeUnsigned();
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("numArrayNodeReps %d", 
                    numArrayNodeReps));
            if (numArrayNodeReps > 0) {
                rep.arrayNodeTextRepID = new int[numArrayNodeReps];
                rep.arrayNodeChildNodeRepID = new int[numArrayNodeReps];
                rep.arrayNodeNumChildren = new int[numArrayNodeReps];
            } 
            numDoubles = decoder.decodeUnsigned();
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("numDoubles %d", numDoubles));
            if (numDoubles > 0) {
                rep.doubles = new double[numDoubles];
            }
            numDocNodeReps = decoder.decodeUnsigned();
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("numDocNodeReps %d", numDocNodeReps));
            if (numDocNodeReps > 0) {
                rep.docNodeTextRepID = new int[numDocNodeReps];
                rep.docNodeChildNodeRepID = new int[numDocNodeReps];
                rep.docNodeNumChildren = new int[numDocNodeReps];
            }
        } 
        else {
            // compat
            if (rep.numNodeReps > 0) {
                rep.nodes = new NodeImpl[rep.numNodeReps];
                rep.nodeOrdinal = new long[rep.numNodeReps];
                rep.nodeKind = new byte[rep.numNodeReps];
                rep.nodeRepID = new int[rep.numNodeReps];
                rep.nodeParentNodeRepID = new int[rep.numNodeReps];
            }
            numElemNodeReps = decoder.decodeUnsigned();
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("numElemNodeReps %d", 
                    numElemNodeReps));
            if (numElemNodeReps > 0) {
                rep.elemNodeNodeNameRepID = new int[numElemNodeReps];
                rep.elemNodeAttrNodeRepID = new int[numElemNodeReps];
                rep.elemNodeChildNodeRepID = new int[numElemNodeReps];
                rep.elemNodeElemDeclRepID = new int[numElemNodeReps];
                rep.elemNodeNumAttributes = new int[numElemNodeReps];
                rep.elemNodeNumDefaultAttrs = new int[numElemNodeReps];
                rep.elemNodeNumChildren = new int[numElemNodeReps];
                rep.elemNodeFlags = new int[numElemNodeReps];
            }
            numAttrNodeReps = decoder.decodeUnsigned();
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("numAttrNodeReps %d", 
                    numAttrNodeReps));
            if (numAttrNodeReps > 0) {
                rep.attrNodeNodeNameRepID = new int[numAttrNodeReps];
                rep.attrNodeTextRepID = new int[numAttrNodeReps];
                rep.attrNodeAttrDeclRepID = new int[numAttrNodeReps];
            }
            rep.numLinkNodeReps = decoder.decodeUnsigned() * 4 / 3;
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("numLinkNodeReps %d", 
                        rep.numLinkNodeReps));
            if (rep.numLinkNodeReps > 0) {
                rep.linkNodeKey = new long[rep.numLinkNodeReps];
                rep.linkNodeNodeCount = new long[rep.numLinkNodeReps];
                rep.linkNodeNodeNameRepID = new int[rep.numLinkNodeReps];
                rep.linkNodeNodeRepID = new int[rep.numLinkNodeReps];
            }
            numDocNodeReps = decoder.decodeUnsigned();
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("numDocNodeReps %d", numDocNodeReps));
            if (numDocNodeReps > 0) {
                rep.docNodeTextRepID = new int[numDocNodeReps];
                rep.docNodeChildNodeRepID = new int[numDocNodeReps];
                rep.docNodeNumChildren = new int[numDocNodeReps];
            }
            numPINodeReps = decoder.decodeUnsigned();
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("numPINodeReps %d", numPINodeReps));
            if (numPINodeReps > 0) {
                rep.piNodeTargetAtom = new int[numPINodeReps];
                rep.piNodeTextRepID = new int[numPINodeReps];
            }
            rep.numNSNodeReps = decoder.decodeUnsigned();
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("numNSNodeReps %d", 
                    rep.numNSNodeReps));
            if (rep.numNSNodeReps > 0) {
                rep.nsNodeOrdinal = new long[rep.numNSNodeReps];
                rep.nsNodePrevNSNodeRepID = new int[rep.numNSNodeReps];
                rep.nsNodePrefixAtom = new int[rep.numNSNodeReps];
                rep.nsNodeUriAtom = new int[rep.numNSNodeReps];
            }
        }

        rep.numPermNodeReps = decoder.decodeUnsigned();
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("numPermNodeReps %d", 
                    rep.numPermNodeReps));
        if (rep.numPermNodeReps > 0) {
            rep.permNodeOrdinal = new long[rep.numPermNodeReps];
            rep.permNodePrevPermNodeRepID = new int[rep.numPermNodeReps];
            rep.permNodeCapability = new Capability[rep.numPermNodeReps];
            rep.permNodeRoleId = new long[rep.numPermNodeReps];
        }
        // uri atoms
        rep.uriTextRepID = 0;
        decodeText(rep, decoder, rep.atomLimit);
        // collection atoms
        rep.colsTextRepID = rep.numTextReps;
        decodeText(rep, decoder, rep.atomLimit);
        // nodes
        int nextDocNodeRep = 0;
        int nextElemNodeRep = 0;
        int nextAttrNodeRep = 0;
        int nextPINodeRep = 0;
        int nextNSNodeRep = 0;
        int nextPermNodeRep = 0;
        int parentNodeRepID = 0;
        int nextArrayNodeRep = 0;
        int nextDouble = 0;
        long lastNSNodeRepOrdinal = 0;
        long lastPermNodeRepOrdinal = 0;
        for (int i = 0; i < rep.numNodeReps; i++) {
            rep.nodeKind[i] = (byte)decoder.decodeUnsigned(4);
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("  nodeKind[%d] %s", i, 
                        rep.nodeKind[i]));
            //assert (rep.nodeKind[i] != NodeKind.NULL);
            parentNodeRepID += decoder.decodeUnsigned();
            if (LOG.isTraceEnabled())
                LOG.trace(String.format("  parentNodeRepID[%d] %d", i, 
                        parentNodeRepID));
            assert (parentNodeRepID <= i);
            if (parentNodeRepID == i)
                rep.nodeParentNodeRepID[i] = Integer.MAX_VALUE;
            else {
                rep.nodeParentNodeRepID[i] = parentNodeRepID;
                assert (rep.nodeKind[parentNodeRepID] == NodeKind.ELEM || 
                        rep.nodeKind[parentNodeRepID] == NodeKind.DOC || 
                        rep.nodeKind[parentNodeRepID] == NodeKind.ARRAY || 
                        rep.nodeKind[parentNodeRepID] == NodeKind.OBJECT || 
                        rep.nodeKind[parentNodeRepID] == NodeKind.LINK);
                int parentRepID = rep.nodeRepID[parentNodeRepID];
                switch (rep.nodeKind[parentNodeRepID]) {
                case NodeKind.ELEM: {
                    switch (rep.nodeKind[i]) {
                    case NodeKind.ATTR:
                        if (rep.elemNodeAttrNodeRepID[parentRepID] == 
                            Integer.MAX_VALUE)
                            rep.elemNodeAttrNodeRepID[parentRepID] = i;
                        assert (rep.elemNodeAttrNodeRepID[parentRepID] + 
                                rep.elemNodeNumAttributes[parentRepID] == i);
                        ++rep.elemNodeNumAttributes[parentRepID];
                        break;
                    default:
                        if (rep.elemNodeChildNodeRepID[parentRepID] == 
                            Integer.MAX_VALUE)
                            rep.elemNodeChildNodeRepID[parentRepID] = i;
                        assert (rep.elemNodeChildNodeRepID[parentRepID] + 
                                rep.elemNodeNumChildren[parentRepID] == i);
                        ++rep.elemNodeNumChildren[parentRepID];
                    }
                    break;
                }
                case NodeKind.DOC: {
                    if (rep.docNodeChildNodeRepID[parentNodeRepID] == 
                            Integer.MAX_VALUE)
                        rep.docNodeChildNodeRepID[parentNodeRepID] = i;
                    assert (rep.docNodeChildNodeRepID[parentNodeRepID] + 
                            rep.docNodeNumChildren[parentNodeRepID] == i);
                    ++rep.docNodeNumChildren[parentNodeRepID];
                    break;
                }
                case NodeKind.ARRAY:
                case NodeKind.OBJECT: {
                    if (rep.arrayNodeChildNodeRepID[parentRepID] == 
                            Integer.MAX_VALUE)
                        rep.arrayNodeChildNodeRepID[parentRepID] = i;
                    assert (rep.arrayNodeChildNodeRepID[parentRepID] + 
                            rep.arrayNodeNumChildren[parentRepID] == i);
                    ++rep.arrayNodeNumChildren[parentRepID];
                    break;
                }
                default:
                    break;
                }
            }
            switch (rep.nodeKind[i]) {
            case NodeKind.ELEM: {
                int j = nextElemNodeRep++;
                rep.nodeRepID[i] = j;
                assert (j < numElemNodeReps);
                rep.elemNodeNodeNameRepID[j] = decoder.decodeUnsigned();
                rep.elemNodeAttrNodeRepID[j] = Integer.MAX_VALUE;
                rep.elemNodeChildNodeRepID[j] = Integer.MAX_VALUE;
                rep.elemNodeElemDeclRepID[j] = Integer.MAX_VALUE;
                rep.elemNodeNumAttributes[j] = 0;
                rep.elemNodeNumDefaultAttrs[j] = 0;
                rep.elemNodeNumChildren[j] = 0;
                rep.elemNodeFlags[j] = 0;
                if (rep.elemNodeNodeNameRepID[j] >= numNodeNameReps) {
                    rep.elemNodeNumDefaultAttrs[j] = 
                            rep.elemNodeNodeNameRepID[j] / numNodeNameReps;
                    rep.elemNodeNodeNameRepID[j] =
                            rep.elemNodeNodeNameRepID[j] % numNodeNameReps;
                }
                break;
            }
            case NodeKind.ATTR: {
                assert (parentNodeRepID < i);
                assert (rep.nodeKind[parentNodeRepID] == NodeKind.ELEM);
                rep.nodeRepID[i] = nextAttrNodeRep++;
                assert (rep.nodeRepID[i] < numAttrNodeReps);
                rep.attrNodeNodeNameRepID[rep.nodeRepID[i]] = 
                        decoder.decodeUnsigned();
                assert (rep.attrNodeNodeNameRepID[rep.nodeRepID[i]] 
                        < numNodeNameReps);
                if (rep.attrNodeNodeNameRepID[rep.nodeRepID[i]] == 
                        xmlSpaceNodeNameRepID)
                    rep.elemNodeFlags[rep.nodeRepID[parentNodeRepID]] |= 
                    xmlSpaceAttrPresentFlag;
                else if (rep.attrNodeNodeNameRepID[rep.nodeRepID[i]] == 
                        xmlLangNodeNameRepID)
                    rep.elemNodeFlags[rep.nodeRepID[parentNodeRepID]] |= 
                    xmlLangAttrPresentFlag;
                else if (rep.attrNodeNodeNameRepID[rep.nodeRepID[i]] == 
                        xmlBaseNodeNameRepID)
                    rep.elemNodeFlags[rep.nodeRepID[parentNodeRepID]] |= 
                    xmlBaseAttrPresentFlag;
                else if (rep.attrNodeNodeNameRepID[rep.nodeRepID[i]] == 
                        xsiTypeNodeNameRepID)
                    rep.elemNodeFlags[rep.nodeRepID[parentNodeRepID]] |= 
                    xsiTypeAttrPresentFlag;
                rep.attrNodeTextRepID[rep.nodeRepID[i]] = rep.numTextReps;
                decodeText(rep, decoder, rep.atomLimit);
                rep.attrNodeAttrDeclRepID[rep.nodeRepID[i]] = 
                        Integer.MAX_VALUE;
                break;
            }
            case NodeKind.TEXT: {
                rep.nodeRepID[i] = rep.numTextReps;
                decodeText(rep, decoder, rep.atomLimit);
                break;
            }
            case NodeKind.BINARY: {
                 rep.nodeRepID[i] = 0;
                 int nbytes = decoder.decodeUnsigned();
                 if (nbytes > MAX_BINARY_BYTES) { // large binary
                     rep.binaryKey = decoder.decode64bits();
                     rep.binaryOffset = decoder.decodeUnsignedLong();
                     rep.binarySize = decoder.decodeUnsignedLong();
                     rep.binaryOrigLen = decoder.decodeUnsignedLong();
                     rep.binaryPathAtom = decoder.decodeUnsigned();
                 } else {
                     decodeBinary(decoder, rep, nbytes);
                 }
                break;
            }
            case NodeKind.PI: {
                int piNodeRep = rep.nodeRepID[i] = nextPINodeRep++;
                assert (piNodeRep < numPINodeReps);
                int targetAtom = rep.piNodeTargetAtom[piNodeRep] = 
                        decoder.decodeUnsigned();
                assert (targetAtom < rep.atomLimit);
                rep.piNodeTextRepID[piNodeRep] = rep.numTextReps;
                decodeText(rep, decoder, rep.atomLimit);
                break;
            }
            case NodeKind.LINK: {
                long key = decoder.decode64bits();
                int linkNodeRep = (int)(key % rep.numLinkNodeReps);
                while (true) {
                    if (rep.linkNodeKey[linkNodeRep] == 0) {
                        rep.nodeRepID[i] = linkNodeRep;
                        rep.linkNodeKey[linkNodeRep] = key;
                        rep.linkNodeNodeCount[linkNodeRep] = 
                                decoder.decodeUnsignedLong();
                        rep.linkNodeNodeNameRepID[linkNodeRep] = 
                                decoder.decodeUnsigned();
                        assert (rep.linkNodeNodeNameRepID[linkNodeRep] < 
                                numNodeNameReps);
                        rep.linkNodeNodeRepID[linkNodeRep] = i;
                        break;
                    }
                    linkNodeRep = hashWrap(linkNodeRep + 1, 
                            rep.numLinkNodeReps);
                }
                break;
            }
            case NodeKind.COMMENT: {
                rep.nodeRepID[i] = rep.numTextReps;
                decodeText(rep, decoder, rep.atomLimit);
                break;
            }
            case NodeKind.DOC: {
                int docNode = rep.nodeRepID[i] = nextDocNodeRep++;
                assert (docNode < numDocNodeReps);
                rep.docNodeTextRepID[i] = rep.numTextReps;
                decodeText(rep, decoder, rep.atomLimit);
                rep.docNodeChildNodeRepID[docNode] = Integer.MAX_VALUE;
                rep.docNodeNumChildren[docNode] = 0;
                break;
            }
            case NodeKind.NS: {
                int nsNode = rep.nodeRepID[i] = nextNSNodeRep++;
                assert (nsNode < rep.numNSNodeReps);
                lastNSNodeRepOrdinal = rep.nsNodeOrdinal[nsNode] = 
                        lastNSNodeRepOrdinal + decoder.decodeUnsignedLong();
                rep.nsNodePrevNSNodeRepID[nsNode] = rep.nodeRepID[i] - 
                        decoder.decodeUnsigned() - 1;
                assert (rep.nsNodePrevNSNodeRepID[nsNode] < rep.numNSNodeReps 
                        || rep.nsNodePrevNSNodeRepID[nsNode] == 
                            Integer.MAX_VALUE);
                rep.nsNodePrefixAtom[nsNode] = decoder.decodeUnsigned() - 1;
                assert (rep.nsNodePrefixAtom[nsNode] < rep.atomLimit || 
                        rep.nsNodePrefixAtom[nsNode] == Integer.MAX_VALUE);
                rep.nsNodeUriAtom[nsNode] = decoder.decodeUnsigned() - 1;
                assert (rep.nsNodeUriAtom[nsNode] < rep.atomLimit || 
                        rep.nsNodeUriAtom[nsNode] == Integer.MAX_VALUE);
                break;
            }
            case NodeKind.PERM: {
                int permNode = rep.nodeRepID[i] = nextPermNodeRep++;
                assert (permNode < rep.numPermNodeReps);
                lastPermNodeRepOrdinal = rep.permNodeOrdinal[permNode] = 
                        lastPermNodeRepOrdinal
                        + decoder.decodeUnsignedLong();
                long prevPermNode = rep.permNodePrevPermNodeRepID[permNode] = 
                        permNode - decoder.decodeUnsigned() - 1;
                assert (prevPermNode < rep.numPermNodeReps || 
                        prevPermNode == Integer.MAX_VALUE);
                Capability capability = rep.permNodeCapability[permNode] = 
                        Capability.values()[decoder.decodeUnsigned(4)];
                assert (capability != Capability.NULL);
                long roleId = rep.permNodeRoleId[permNode] = 
                        decoder.decode64bits();
                assert (roleId < Long.MAX_VALUE);
                break;
            }
            case NodeKind.NULL: {
                switch (decoder.decodeUnsigned(3)) {
                case 1: {
                    rep.nodeKind[i] = NodeKind.BOOLEAN;
                    rep.nodeRepID[i] = 0;
                    break;
                }
                case 2: {
                    rep.nodeKind[i] = NodeKind.BOOLEAN;
                    rep.nodeRepID[i] = 1;
                    break;
                }
                case 3: {
                    rep.nodeKind[i] = NodeKind.NUMBER;
                    rep.nodeRepID[i] = nextDouble++;
                    assert(rep.nodeRepID[i] < numDoubles);
                    rep.doubles[rep.nodeRepID[i]] = decoder.decodeDouble();
                    break;
                }
                case 4: {
                    rep.nodeKind[i] = NodeKind.ARRAY;
                    rep.nodeRepID[i] = nextArrayNodeRep++;
                    assert(rep.nodeRepID[i] < numArrayNodeReps);
                    rep.arrayNodeTextRepID[rep.nodeRepID[i]] = 
                        Integer.MAX_VALUE;
                    rep.arrayNodeChildNodeRepID[rep.nodeRepID[i]] = 
                        Integer.MAX_VALUE;
                    rep.arrayNodeNumChildren[rep.nodeRepID[i]] = 0;
                    break; 
                }
                case 5: {
                    rep.nodeKind[i] = NodeKind.OBJECT;
                    rep.nodeRepID[i] = nextArrayNodeRep++;
                    assert(rep.nodeRepID[i] < numArrayNodeReps);
                    rep.arrayNodeTextRepID[rep.nodeRepID[i]] = rep.numTextReps;
                    rep.arrayNodeChildNodeRepID[rep.nodeRepID[i]] = 
                        Integer.MAX_VALUE;
                    rep.arrayNodeNumChildren[rep.nodeRepID[i]] = 0;
                    int numKeys = decoder.decodeUnsigned();
                    addText(rep,numKeys);
                    int atomLimit = rep.atomLimit;
                    for (int j=0; j<numKeys; ++j) {
                        int atom = decoder.decodeUnsigned();
                        assert(atom<atomLimit);
                        if (atom>=atomLimit) {
                            bad="atom";
                            if (LOG.isTraceEnabled())
                                LOG.trace(String.format(
                                    "bad atom %d atomLimit %d",
                                    atom,atomLimit));
                        }
                        rep.textReps[rep.numTextReps++] = atom;
                    }
                    break;
                }
                default:
                    break;
                }
                break;
            }
            default:
                break;
            }
        }
        if (rep.numNodeReps > 0) {
            assignOrdinals(rep);
        }
        return rep;
    }

    private void decodeBinary(Decoder decoder, ExpandedTree rep, int nbytes) 
    throws IOException {
        int nwords = ((nbytes+3)/4);
        if (nwords <= 0) {
            LOG.error("nbytes=" + nbytes + ", nwords=" + nwords);
        }
        rep.binaryData = new int[nwords];
        decoder.decode(rep.binaryData, 1, nwords);
    }

    private void assignOrdinals(ExpandedTree rep) {
        long ordinal = 0;
        int nodeID = 0;
        if (rep.nodeKind[0] == NodeKind.LINK) {
            rep.ordinal = rep.linkNodeNodeCount[rep.nodeRepID[0]];
            rep.nodeOrdinal[0] = 0;
            nodeID = 1;
        }
        while (nodeID != Integer.MAX_VALUE) {
            rep.nodeOrdinal[nodeID] = ordinal++;
            switch (rep.nodeKind[nodeID]) {
            case NodeKind.ELEM: {
                int elemID = rep.nodeRepID[nodeID];
                for (int i = 0; i < rep.elemNodeNumAttributes[elemID]; i++) {
                    int attrNodeID = rep.elemNodeAttrNodeRepID[elemID] + i;
                    rep.nodeOrdinal[attrNodeID] = ordinal++;
                }
                int childNodeID = rep.elemNodeChildNodeRepID[elemID];
                if (childNodeID != Integer.MAX_VALUE) {
                    nodeID = childNodeID;
                    continue;
                }
                break;
            }
            case NodeKind.LINK: {
                int linkID = rep.nodeRepID[nodeID];
                ordinal += rep.linkNodeNodeCount[linkID] - 1;
                break;
            }
            case NodeKind.DOC: {
                int docID = rep.nodeRepID[nodeID];
                int childNodeID = rep.docNodeChildNodeRepID[docID];
                if (childNodeID != Integer.MAX_VALUE) {
                    nodeID = childNodeID;
                    continue;
                }
                break;
            }
            case NodeKind.ARRAY:
            case NodeKind.OBJECT: {
                int docID = rep.nodeRepID[nodeID];
                int childNodeID = rep.arrayNodeChildNodeRepID[docID];
                if (childNodeID != Integer.MAX_VALUE) {
                    nodeID = childNodeID;
                    continue;
                }
                break;
            }
            default:
                break;
            }
            int parentNodeID = rep.nodeParentNodeRepID[nodeID];
            for (;;) {
                if (parentNodeID == Integer.MAX_VALUE) {
                    nodeID = Integer.MAX_VALUE;
                    break;
                }
                if (rep.nodeKind[parentNodeID] == NodeKind.ELEM) {
                    int elemID = rep.nodeRepID[parentNodeID];
                    if (++nodeID < rep.elemNodeChildNodeRepID[elemID] + 
                            rep.elemNodeNumChildren[elemID])
                        break;
                } else if (rep.nodeKind[parentNodeID] == NodeKind.DOC) {
                    int docID = rep.nodeRepID[parentNodeID];
                    if (++nodeID < rep.docNodeChildNodeRepID[docID] + 
                            rep.docNodeNumChildren[docID])
                        break;
                } else if (rep.nodeKind[parentNodeID] == NodeKind.ARRAY ||
                           rep.nodeKind[parentNodeID] == NodeKind.OBJECT) {
                    int docID = rep.nodeRepID[parentNodeID];
                    if (++nodeID < rep.arrayNodeChildNodeRepID[docID] + 
                            rep.arrayNodeNumChildren[docID])
                        break;
                }
                nodeID = parentNodeID;
                parentNodeID = rep.nodeParentNodeRepID[nodeID];
            }
        }
        for (int j = rep.numNodeReps - rep.numNSNodeReps - rep.numPermNodeReps;
             j < rep.numNodeReps; 
             ++j)
            rep.nodeOrdinal[j] = ordinal++;
        for (int k = rep.numNodeReps - rep.numPermNodeReps; 
             k < rep.numNodeReps; 
             ++k)
            rep.nodeOrdinal[k] = ordinal++;
        // TODO: compared performance
        if (Boolean.getBoolean("xcc.decode.atoms")) {
            for (int x = 0; x < rep.atomLimit; ++x) rep.atomString(x);
        }
    }

    public static int hashWrap(int x, int y) {
        return (x < y) ? x : x - y;
    }
}
