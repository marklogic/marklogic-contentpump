package com.marklogic;

import java.io.File;
import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;

import com.marklogic.dom.DocumentImpl;
import com.marklogic.tree.ExpandedTree;

public class TestDocumentImpl extends TestCase {
    boolean verbose = false;
    String testData = "src/testdata";


    public void testGetDocumentURI() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
                + "3docForest", "00000002"), false);
        assertEquals(3, trees.size());

        StringBuffer expected = new StringBuffer();
        StringBuffer actual = new StringBuffer();
        for (int i = 0; i < trees.size(); i++) {
            ExpandedTree t = trees.get(i);
            expected.append(t.getDocumentURI());
            DocumentImpl d = new DocumentImpl(t, i);
            actual.append(d.getDocumentURI());
        }
        assertEquals(expected.toString(), actual.toString());
    }
    
    public void testGetNodeNameAndFirstChild() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
                + "3docForest", "00000002"), false);
        assertEquals(3, trees.size());

        StringBuffer expected = new StringBuffer();
        StringBuffer actual = new StringBuffer();
        for (int i = 0; i < trees.size(); i++) {
            ExpandedTree t = trees.get(i);
            expected.append("#document").append("root");
            DocumentImpl d = new DocumentImpl(t, 0);
            actual.append(d.getNodeName());
            String lname = d.getFirstChild().getLocalName();
            System.out.println(lname);
            actual.append(lname);
        }
        assertEquals(expected.toString(), actual.toString());
        
    }
    
    public void testJavaDomSample() throws IOException {
        Utils.readXMLasDOMDocument(new File(testData, "doc1.xml"));
    }
}
