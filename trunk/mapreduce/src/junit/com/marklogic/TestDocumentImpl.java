package com.marklogic;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

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
    
//    public void testJavaDomSample() throws IOException {
//        Document doc = Utils.readXMLasDOMDocument(new File(testData, "doc1.xml"));
//        System.out.println("JAVA DOM Root element :"
//            + doc.getDocumentElement().getNodeName());
//
//        NodeList children = doc.getChildNodes();
//        
//        StringBuilder sb = new StringBuilder();
//        walk(children, sb);
//        System.out.println(sb.toString());
//        for (int temp = 0; temp < children.getLength(); temp++) {
//
//            Node nNode = children.item(temp);
//
//            System.out
//                .println("\nCurrent Element :" + nNode.getNodeName());
//
//            if (nNode.getNodeType() == Node.ELEMENT_NODE) {
//
//                Element eElement = (Element) nNode;
//                System.out.println(eElement.getNodeName());
//                System.out.println("contry id : "
//                    + eElement.getAttribute("id"));
//                System.out.println("Country : "
//                    + eElement.getElementsByTagName("country").item(0)
//                        .getTextContent());
//                System.out.println("Last Name : "
//                    + eElement.getElementsByTagName("lastname").item(0)
//                        .getTextContent());
//                System.out.println("Nick Name : "
//                    + eElement.getElementsByTagName("nickname").item(0)
//                        .getTextContent());
//                System.out.println("Salary : "
//                    + eElement.getElementsByTagName("salary").item(0)
//                        .getTextContent());

//            }
//        }
//    }
    
    private void walkDOM (NodeList nodes, StringBuilder sb) {
        for(int i=0; i<nodes.getLength(); i++) {
            Node child = nodes.item(i);
            sb.append(child.getNodeName()).append("#");
            if(child.hasChildNodes()) {
                walkDOM(child.getChildNodes(), sb);
            }
        }
    }
    

    
    public void testNodeNameChildNodes() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
                + "3docForest", "00000002"), false);
        assertEquals(3, trees.size());

        StringBuilder expected = new StringBuilder();
        StringBuilder actual = new StringBuilder();
        for (int i = 0; i < trees.size(); i++) {
            ExpandedTree t = trees.get(i);
            String uri = t.getDocumentURI();
            expected.append(uri);
            Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
            NodeList children = doc.getChildNodes();
            walkDOM(children, expected);
            DocumentImpl d = new DocumentImpl(t, 0);
            NodeList eChildren = d.getChildNodes();
            actual.append(uri);
            walkDOM (eChildren, actual);
            
            expected.append("\n");
            actual.append("\n");
        }
        System.out.println(expected.toString());
        System.out.println(actual.toString());
        assertEquals(expected.toString(), actual.toString());
 
    }
}
