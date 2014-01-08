package com.marklogic;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.marklogic.dom.DocumentImpl;
import com.marklogic.dom.ElementImpl;
import com.marklogic.tree.ExpandedTree;

@RunWith(value = Parameterized.class)
public class TestDocumentImpl extends AbstractTestCase {
	public TestDocumentImpl(ForestData fd) {
        super(fd);
    }

    boolean verbose = false;
	
//	String testData = "src/testdata/ns-prefix";
//    String forest = "ns-prefix-forest";
//    String stand = "00000001";
//    int num = 23;

/*	String testData = "src/testdata/dom-core-test";
    String forest = "DOM-test-forest";
    String stand = "00000002";
    int num = 16; */  
    

/*    String testData = "src/testdata/3doc-test";
    String forest = "3docForest";
    String stand = "00000002";
    int num = 3;   */

	@Test
    public void testGetDocumentURI() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
                + forest, stand), false);
        assertEquals(num, trees.size());

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
    
	@Test
    public void testGetNodeNameAndFirstAndLastChild() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
            		+ forest, stand), false);
        assertEquals(num, trees.size());

        StringBuffer expected = new StringBuffer();
        StringBuffer actual = new StringBuffer();
        for (int i = 0; i < trees.size(); i++) {
            ExpandedTree t = trees.get(i);
        	String uri = t.getDocumentURI();
        	
        	Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
            expected.append(uri);
            expected.append("#FIRSTCHILD##").
            		 append(doc.getFirstChild().getNodeName()).append("#").append("\n");
            expected.append("#LASTCHILD##").
            		 append(doc.getLastChild().getNodeName()).append("#").append("\n");
            
            DocumentImpl d = new DocumentImpl(t, 0);
            actual.append(uri);
            String lname = d.getFirstChild().getNodeName();
            actual.append("#FIRSTCHILD##").append(lname).append("#").append("\n");
            lname = d.getLastChild().getNodeName();
            actual.append("#LASTCHILD##").append(lname).append("#").append("\n");
        }
        System.out.println(expected.toString());
        System.out.println(actual.toString());
        assertEquals(expected.toString(), actual.toString());
    }
    
    @Test
    public void testGetPrefix() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
            		+ forest, stand), false);
        assertEquals(num, trees.size());

        StringBuffer expected = new StringBuffer();
        StringBuffer actual = new StringBuffer();
        
        for (int i = 0; i < trees.size(); i++) {
            ExpandedTree t = trees.get(i);
        	String uri = t.getDocumentURI();
        	
        	Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        	        	
            expected.append("\n").append(uri).append("\n");
        	Queue<NodeList> q = new LinkedList<NodeList>();
        	if (doc.hasChildNodes()) q.add(doc.getChildNodes());
        	while (!q.isEmpty()) {
        		NodeList nl = q.poll();
        		for (int k=0; k<nl.getLength(); k++) {
        			if (nl.item(k).hasChildNodes()) 
        				q.add(nl.item(k).getChildNodes());
                    if (nl.item(k).getNodeType() == Node.ELEMENT_NODE)
                    expected.append("#CHILD##").
           		     append(nl.item(k).getNodeName()).append("#").append("\n");
        		}
        	}
        	
            q.clear();
            DocumentImpl d = new DocumentImpl(t, 0);
            actual.append("\n").append(uri).append("\n");
        	if (d.hasChildNodes()) q.add(d.getChildNodes());
        	while (!q.isEmpty()) {
        		NodeList nl = q.poll();
        		for (int k=0; k<nl.getLength(); k++) {
        			if (nl.item(k).hasChildNodes()) 
        				q.add(nl.item(k).getChildNodes());
                    if (nl.item(k).getNodeType() == Node.ELEMENT_NODE)
        			actual.append("#CHILD##").
           		     append(nl.item(k).getNodeName()).append("#").append("\n");
        		}
        		
        	}
            
        	expected.append("#");
        	actual.append("#");
        }
        System.out.println(expected.toString());
        System.out.println(actual.toString());
        assertEquals(expected.toString(), actual.toString());
    }
    
    @Test
    public void testGetOwnerDocumentBaseURI() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
            		+ forest, stand), false);
        assertEquals(num, trees.size());

        StringBuffer expected = new StringBuffer();
        StringBuffer actual = new StringBuffer();
        for (int i = 0; i < trees.size(); i++) {
            ExpandedTree t = trees.get(i);
        	String uri = t.getDocumentURI();
        	
        	Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        	if (doc == null) continue;
        	
            expected.append(doc.getNodeName());
            expected.append("#OWNDERDOCROOT##").
            		 append(doc.getOwnerDocument() == null).append("#");
            expected.append("#OWNDERDOCCHILD##").
            		 append(doc.getFirstChild().getOwnerDocument().getNodeName()).append("#");
            
            DocumentImpl d = new DocumentImpl(t, 0);
            actual.append(d.getNodeName());
            actual.append("#OWNDERDOCROOT##").
   		 		   append(d.getOwnerDocument() == null).append("#");
            actual.append("#OWNDERDOCCHILD##").
   		 		   append(d.getFirstChild().getOwnerDocument().getNodeName()).append("#");
            
            String expectedUri = doc.getBaseURI();
            String actualUri = d.getBaseURI();
            
            if (!expectedUri.contains(actualUri)) {
                expected.append("#BASEURIROOT##").
       		 			 append(expectedUri).append("#");
                actual.append("#BASEURIROOT##").
		 			 append(actualUri).append("#");
            }
        }
        System.out.println(expected.toString());
        System.out.println(actual.toString());
        assertEquals(expected.toString(), actual.toString());
    }
    
    @Test
    public void testGetLocalNameGetNamespaceURI() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
            		+ forest, stand), false);
        assertEquals(num, trees.size());

        StringBuffer expected = new StringBuffer();
        StringBuffer actual = new StringBuffer();
        for (int i = 0; i < trees.size(); i++) {
            ExpandedTree t = trees.get(i);
        	String uri = t.getDocumentURI();
        	
        	Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        	if (doc == null) continue;
            DocumentImpl d = new DocumentImpl(t, 0);
        	
            expected.append(d.getBaseURI()).append("\n");
            actual.append(d.getBaseURI()).append("\n");
            
            NodeList children = doc.getFirstChild().getChildNodes();
            for (int k = 0; k < children.getLength(); k++) {
              Node curr = children.item(k);
//              if(Utils.isWhitespaceNode(curr)) continue;
              expected.append("#NODENAME##").append(curr.getNodeName()).append("#").append("\n");
              String nodename = curr.getNodeName();
              int tok = nodename.indexOf(':'); 
              String prefix = (tok == -1)?null:nodename.substring(0, tok);
              String namespace = (tok == -1)?null:curr.lookupNamespaceURI(prefix);
              String localname = (tok == -1)?nodename:nodename.substring(tok);
              expected.append("#LOCALNAME##").append(localname).append("#").append("\n");
              expected.append("#URI##").append(namespace).append("#").append("\n");
              expected.append("\n");
            }
            children = d.getFirstChild().getChildNodes();
            for (int k = 0; k < children.getLength(); k++) {
              Node curr = children.item(k);
              actual.append("#NODENAME##").append(curr.getNodeName()).append("#").append("\n");
              actual.append("#LOCALNAME##").append(curr.getLocalName()).append("#").append("\n");
              actual.append("#URI##").append(curr.getNamespaceURI()).append("#").append("\n");
              actual.append("\n");
            }
        }
        System.out.println(expected.toString());
        System.out.println(actual.toString());
        assertEquals(expected.toString(), actual.toString());
    }
    
    @Test
    public void testGetElementByTagName() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
            		+ forest, stand), false);
        assertEquals(num, trees.size());

        StringBuffer expected = new StringBuffer();
        StringBuffer actual = new StringBuffer();
        
        String tags[] = {"*","country","capital","h:capital","text","body","head","html:head"};
        
        for (int s = 0; s < tags.length; s++){
        	expected.append("#TAG#").append(tags[s]).append("\n");
        	actual.append("#TAG#").append(tags[s]).append("\n");
        for (int i = 0; i < trees.size(); i++) {
            ExpandedTree t = trees.get(i);
        	String uri = t.getDocumentURI();
        	
        	expected.append("#URI##").append(uri).append("#\n");
        	actual.append("#URI##").append(uri).append("#\n");
        	
        	Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        	if (doc == null) continue;
        	int firstelement = 0;
        	while (doc.getChildNodes().item(firstelement).getNodeType() != Node.ELEMENT_NODE) firstelement++;
        	Element rootExpected = (Element) doc.getChildNodes().item(firstelement);
        	NodeList elementsExpected = rootExpected.getElementsByTagName(tags[s]);
        	expected.append("#NUM##").append(elementsExpected.getLength()).
        			append("#").append("\n");
        	ArrayList<Node> esort = new ArrayList<Node>();
        	for (int j = 0; j < elementsExpected.getLength(); j++) {
        		esort.add(elementsExpected.item(j));
        	}
        	Collections.sort(esort, new Comparator<Node>() {
                @Override
                public int compare(Node  n1, Node  n2)
                {
                	return  n1.getNodeName().compareTo(n2.getNodeName());
                }
            });
        	for (int j = 0; j < esort.size(); j++) {
        		Node curr = esort.get(j);
        		expected.append("#I##").append(j).append("#").
        				append("#NODENAME##").append(curr.getNodeName()).append("#").
        				append("\n");
        	}
        		            
            DocumentImpl d = new DocumentImpl(t, 0);
            firstelement = 0;
        	while (d.getChildNodes().item(firstelement).getNodeType() != Node.ELEMENT_NODE) firstelement++;
            ElementImpl rootActual = (ElementImpl) d.getChildNodes().item(firstelement);
        	NodeList elementsActual = rootActual.getElementsByTagName(tags[s]);
        	actual.append("#NUM##").append(elementsActual.getLength()).
			 	append("#").append("\n");
        	ArrayList<Node> asort = new ArrayList<Node>();
        	for (int j = 0; j < elementsActual.getLength(); j++) {
        		asort.add(elementsActual.item(j));
        	}
        	Collections.sort(asort, new Comparator<Node>() {
                @Override
                public int compare(Node  n1, Node  n2)
                {
                	return  n1.getNodeName().compareTo(n2.getNodeName());
                }
            });
        	for (int j = 0; j < asort.size(); j++) {
        		Node curr = asort.get(j);
        		actual.append("#I##").append(j).append("#").
        				append("#NODENAME##").append(curr.getNodeName()).append("#").
        				append("\n");
        	}
         }
    	expected.append("\n");
    	actual.append("\n");
        }
        System.out.println(expected.toString());
        System.out.println(actual.toString());
        assertEquals(expected.toString(), actual.toString());
    }
    
    @Test
    public void testDocGetElementByTagName() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
            		+ forest, stand), false);
        assertEquals(num, trees.size());

        StringBuffer expected = new StringBuffer();
        StringBuffer actual = new StringBuffer();
        
        String tags[] = {"*","country","capital","text","body","head","html:head"};
                
        for (int s = 0; s < tags.length; s++){
        	expected.append("#TAG#").append(tags[s]).append("\n");
        	actual.append("#TAG#").append(tags[s]).append("\n");

            for (int i = 0; i < trees.size(); i++) {
                ExpandedTree t = trees.get(i);
            	String uri = t.getDocumentURI();
            	
            	expected.append("#URI##").append(uri).append("#\n");
            	actual.append("#URI##").append(uri).append("#\n");
            	
            	Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
            	if (doc == null) continue;
            	int firstelement = 0;
            	while (doc.getChildNodes().item(firstelement).getNodeType() != Node.ELEMENT_NODE) firstelement++;
            	Element rootExpected = (Element) doc.getChildNodes().item(firstelement);
            	NodeList elementsExpected = doc.getElementsByTagName(tags[s]);
            	expected.append("#NUM##").append(elementsExpected.getLength()).
            			append("#").append("\n");
            	ArrayList<Node> esort = new ArrayList<Node>();
            	for (int j = 0; j < elementsExpected.getLength(); j++) {
            		esort.add(elementsExpected.item(j));
            	}
            	Collections.sort(esort, new Comparator<Node>() {
                    @Override
                    public int compare(Node  n1, Node  n2)
                    {
                    	return  n1.getNodeName().compareTo(n2.getNodeName());
                    }
                });
            	for (int j = 0; j < esort.size(); j++) {
            		Node curr = esort.get(j);
            		expected.append("#I##").append(j).append("#").
            				append("#NODENAME##").append(curr.getNodeName()).append("#").
            				append("\n");
            	}
            		            
                DocumentImpl d = new DocumentImpl(t, 0);
                firstelement = 0;
            	while (d.getChildNodes().item(firstelement).getNodeType() != Node.ELEMENT_NODE) firstelement++;
                ElementImpl rootActual = (ElementImpl) d.getChildNodes().item(firstelement);
            	NodeList elementsActual = d.getElementsByTagName(tags[s]);
            	actual.append("#NUM##").append(elementsActual.getLength()).
    			 	append("#").append("\n");
            	ArrayList<Node> asort = new ArrayList<Node>();
            	for (int j = 0; j < elementsActual.getLength(); j++) {
            		asort.add(elementsActual.item(j));
            	}
            	Collections.sort(asort, new Comparator<Node>() {
                    @Override
                    public int compare(Node  n1, Node  n2)
                    {
                    	return  n1.getNodeName().compareTo(n2.getNodeName());
                    }
                });
            	for (int j = 0; j < asort.size(); j++) {
            		Node curr = asort.get(j);
            		actual.append("#I##").append(j).append("#").
            				append("#NODENAME##").append(curr.getNodeName()).append("#").
            				append("\n");
            	}
             }
    	expected.append("\n");
    	actual.append("\n");
        }
        System.out.println(expected.toString());
        System.out.println(actual.toString());
        assertEquals(expected.toString(), actual.toString());
    }
    
    @Test
    public void testGetElementByTagNameNS() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
            		+ forest, stand), false);
        assertEquals(num, trees.size());

        StringBuffer expected = new StringBuffer();
        StringBuffer actual = new StringBuffer();
        
        String tags[] = {"*","country","capital","text","body","head","html:head"};
        String nss[] = {"*","http://www.w3.org/TR/html4/"};
        
        for (int n = 0; n < nss.length; n++)
        for (int s = 0; s < tags.length; s++){
        	expected.append("#TAG#").append(tags[s]).append("\n");
        	expected.append("#NS#").append(nss[n]).append("\n");
        	actual.append("#TAG#").append(tags[s]).append("\n");
        	actual.append("#NS#").append(nss[n]).append("\n");

            for (int i = 0; i < trees.size(); i++) {
                ExpandedTree t = trees.get(i);
            	String uri = t.getDocumentURI();
            	
            	expected.append("#URI##").append(uri).append("#\n");
            	actual.append("#URI##").append(uri).append("#\n");
            	
            	Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
            	if (doc == null) continue;
            	int firstelement = 0;
            	while (doc.getChildNodes().item(firstelement).getNodeType() != Node.ELEMENT_NODE) firstelement++;
            	Element rootExpected = (Element) doc.getChildNodes().item(firstelement);
            	NodeList elementsExpected = rootExpected.getElementsByTagNameNS(nss[n],tags[s]);
            	expected.append("#NUM##").append(elementsExpected.getLength()).
            			append("#").append("\n");
            	ArrayList<Node> esort = new ArrayList<Node>();
            	for (int j = 0; j < elementsExpected.getLength(); j++) {
            		esort.add(elementsExpected.item(j));
            	}
            	Collections.sort(esort, new Comparator<Node>() {
                    @Override
                    public int compare(Node  n1, Node  n2)
                    {
                    	return  n1.getNodeName().compareTo(n2.getNodeName());
                    }
                });
            	for (int j = 0; j < esort.size(); j++) {
            		Node curr = esort.get(j);
            		expected.append("#I##").append(j).append("#").
            				append("#NODENAME##").append(curr.getNodeName()).append("#").
            				append("\n");
            	}
            		            
                DocumentImpl d = new DocumentImpl(t, 0);
                firstelement = 0;
            	while (d.getChildNodes().item(firstelement).getNodeType() != Node.ELEMENT_NODE) firstelement++;
                ElementImpl rootActual = (ElementImpl) d.getChildNodes().item(firstelement);
            	NodeList elementsActual = rootActual.getElementsByTagNameNS(nss[n],tags[s]);
            	actual.append("#NUM##").append(elementsActual.getLength()).
    			 	append("#").append("\n");
            	ArrayList<Node> asort = new ArrayList<Node>();
            	for (int j = 0; j < elementsActual.getLength(); j++) {
            		asort.add(elementsActual.item(j));
            	}
            	Collections.sort(asort, new Comparator<Node>() {
                    @Override
                    public int compare(Node  n1, Node  n2)
                    {
                    	return  n1.getNodeName().compareTo(n2.getNodeName());
                    }
                });
            	for (int j = 0; j < asort.size(); j++) {
            		Node curr = asort.get(j);
            		actual.append("#I##").append(j).append("#").
            				append("#NODENAME##").append(curr.getNodeName()).append("#").
            				append("\n");
            	}
             }
    	expected.append("\n");
    	actual.append("\n");
        }
        System.out.println(expected.toString());
        System.out.println(actual.toString());
        assertEquals(expected.toString(), actual.toString());
    }
   
    @Test
    public void testNodeNameChildNodes() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
            		+ forest, stand), false);
        assertEquals(num, trees.size());

        StringBuilder expected = new StringBuilder();
        StringBuilder actual = new StringBuilder();
        for (int i = 0; i < trees.size(); i++) {
            ExpandedTree t = trees.get(i);
            String uri = t.getDocumentURI();
            expected.append(uri);
            Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        	if (doc == null) continue;
            NodeList children = doc.getChildNodes();
            walkDOM(children, expected);
            System.out.println("--------------");
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
    
    @Test
    public void testNextSibling() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
            		+ forest, stand), false);
        assertEquals(num, trees.size());

        StringBuilder expected = new StringBuilder();
        StringBuilder actual = new StringBuilder();
        for (int i = 0; i < trees.size(); i++) {
            ExpandedTree t = trees.get(i);
            String uri = t.getDocumentURI();
            expected.append(uri);
            Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        	if (doc == null) continue;
            NodeList children = doc.getChildNodes();
            walkDOMNextSibling(children, expected);
            DocumentImpl d = new DocumentImpl(t, 0);
            NodeList eChildren = d.getChildNodes();
            actual.append(uri);
            walkDOMNextSibling (eChildren, actual);
            
            expected.append("\n");
            actual.append("\n");
        }
        System.out.println(expected.toString());
        System.out.println(actual.toString());
        assertEquals(expected.toString(), actual.toString());
 
    }
    
    @Test
    public void testPreviousSibling() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
            		+ forest, stand), false);
        assertEquals(num, trees.size());

        StringBuilder expected = new StringBuilder();
        StringBuilder actual = new StringBuilder();
        for (int i = 0; i < trees.size(); i++) {
            ExpandedTree t = trees.get(i);
            String uri = t.getDocumentURI();
            expected.append(uri);
            Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        	if (doc == null) continue;
            NodeList children = doc.getChildNodes();
            walkDOMPreviousSibling(children, expected);
            DocumentImpl d = new DocumentImpl(t, 0);
            NodeList eChildren = d.getChildNodes();
            actual.append(uri);
            walkDOMPreviousSibling (eChildren, actual);
            
            expected.append("\n");
            actual.append("\n");
        }
        System.out.println(expected.toString());
        System.out.println(actual.toString());
        assertEquals(expected.toString(), actual.toString());
    }

    @Test
    public void testParent() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
                new File(testData + System.getProperty("file.separator")
                		+ forest, stand), false);
            assertEquals(num, trees.size());

        StringBuilder expected = new StringBuilder();
        StringBuilder actual = new StringBuilder();
        for (int i = 0; i < trees.size(); i++) {
            ExpandedTree t = trees.get(i);
            String uri = t.getDocumentURI();
            expected.append(uri);
            Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        	if (doc == null) continue;
            NodeList children = doc.getChildNodes();
            walkDOMParent(children, expected);
            DocumentImpl d = new DocumentImpl(t, 0);
            NodeList eChildren = d.getChildNodes();
            actual.append(uri);
            walkDOMParent (eChildren, actual);
            
            expected.append("\n");
            actual.append("\n");
        }
        System.out.println(expected.toString());
        System.out.println(actual.toString());
        assertEquals(expected.toString(), actual.toString());
    }
    
    @Test
    public void testTextContent() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
            		+ forest, stand), false);
        assertEquals(num, trees.size());

        StringBuilder expected = new StringBuilder();
        StringBuilder actual = new StringBuilder();
        for (int i = 0; i < trees.size(); i++) {
            ExpandedTree t = trees.get(i);
            String uri = t.getDocumentURI();
            expected.append(uri);
            Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        	if (doc == null) continue;
            NodeList children = doc.getChildNodes();
            walkDOMTextContent(children, expected);
            DocumentImpl d = new DocumentImpl(t, 0);
            NodeList eChildren = d.getChildNodes();
            actual.append(uri);
            walkDOMTextContent (eChildren, actual);
            
            expected.append("\n");
            actual.append("\n");
        }
        System.out.println(expected.toString());
        System.out.println(actual.toString());
        assertEquals(expected.toString(), actual.toString());
    }
    

    @Test
    public void testAttributes() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
                new File(testData + System.getProperty("file.separator")
                		+ forest, stand), false);
            assertEquals(num, trees.size());

        StringBuilder expected = new StringBuilder();
        StringBuilder actual = new StringBuilder();
        for (int i = 0; i < trees.size(); i++) {
            ExpandedTree t = trees.get(i);
            String uri = t.getDocumentURI();
            expected.append(uri);
            Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        	if (doc == null) continue;
            NodeList children = doc.getChildNodes();
            walkDOMAttr(children, expected);
            DocumentImpl d = new DocumentImpl(t, 0);
            NodeList eChildren = d.getChildNodes();
            actual.append(uri);
            walkDOMAttr (eChildren, actual);
            
            expected.append("\n");
            actual.append("\n");
        }
        System.out.println(expected.toString());
        System.out.println(actual.toString());
        assertEquals(expected.toString(), actual.toString());
    }
    
    @Test
    public void testAttributeNode() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
                new File(testData + System.getProperty("file.separator")
                		+ forest, stand), false);
            assertEquals(num, trees.size());

        StringBuilder expected = new StringBuilder();
        StringBuilder actual = new StringBuilder();
        for (int i = 0; i < trees.size(); i++) {
            ExpandedTree t = trees.get(i);
            String uri = t.getDocumentURI();
            expected.append(uri);
            Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        	if (doc == null) continue;
            NodeList children = doc.getChildNodes();
            walkDOMElem(children, expected);
            DocumentImpl d = new DocumentImpl(t, 0);
            NodeList eChildren = d.getChildNodes();
            actual.append(uri);
            walkDOMElem(eChildren, actual);
            
            expected.append("\n");
            actual.append("\n");
        }
        System.out.println(expected.toString());
        System.out.println(actual.toString());
        assertEquals(expected.toString(), actual.toString());
    }
    
    @Test
    public void testDeepClone() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
                    + forest, stand), false);
        assertEquals(num, trees.size());

    StringBuilder expected = new StringBuilder();
    StringBuilder actual = new StringBuilder();
    StringBuilder clone = new StringBuilder();
    for (int i = 0; i < trees.size(); i++) {
        ExpandedTree t = trees.get(i);
        String uri = t.getDocumentURI();
        expected.append(uri);
        Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        if (doc == null) continue;
        NodeList children = doc.getChildNodes();
        walkDOMElem(children, expected);
        DocumentImpl d = new DocumentImpl(t, 0);
        children = d.getChildNodes();
        actual.append(uri);
        walkDOMElem(children, actual);
        
        Document clonedDoc = (Document) d.cloneNode(true);
        clone.append(uri);
        children = clonedDoc.getChildNodes();
        walkDOMElem(children, clone);
        
        expected.append("\n");
        actual.append("\n");
        clone.append("\n");
    }
    System.out.println(expected.toString());
    System.out.println(actual.toString());
    System.out.println(clone.toString());
    assertEquals(actual.toString(), clone.toString());
    assertEquals(expected.toString(), clone.toString());
    
    }
    
    @Test
    public void testDeepCloneBug25449() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
                    + forest, stand), false);
        assertEquals(num, trees.size());

    StringBuilder expected = new StringBuilder();
    StringBuilder actual = new StringBuilder();
    StringBuilder clone = new StringBuilder();
    for (int i = 0; i < trees.size(); i++) {
        ExpandedTree t = trees.get(i);
        String uri = t.getDocumentURI();
        expected.append(uri);
        Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        if (doc == null) continue;
        NodeList children = doc.getChildNodes();
        walkDOMElem(children, expected);
        DocumentImpl d = new DocumentImpl(t, 0);
        children = d.getChildNodes();
        actual.append(uri);
        walkDOMElem(children, actual);
        
        Document clonedDoc = (Document) d.cloneNode(true);
        clonedDoc = (Document) d.cloneNode(true);
        clone.append(uri);
        children = clonedDoc.getChildNodes();
        walkDOMElem(children, clone);
        
        expected.append("\n");
        actual.append("\n");
        clone.append("\n");
    }
    System.out.println(expected.toString());
    System.out.println(actual.toString());
    System.out.println(clone.toString());
    assertEquals(actual.toString(), clone.toString());
    assertEquals(expected.toString(), clone.toString());
    
    }
    
    @Test
    public void testShallowClone() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
            new File(testData + System.getProperty("file.separator")
                    + forest, stand), false);
        assertEquals(num, trees.size());

    StringBuilder expected = new StringBuilder();
    StringBuilder actual = new StringBuilder();
    StringBuilder clone = new StringBuilder();
    for (int i = 0; i < trees.size(); i++) {
        ExpandedTree t = trees.get(i);
        String uri = t.getDocumentURI();
        expected.append(uri);
        Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        if (doc == null) continue;
        Node root = doc.getDocumentElement();
        expected.append(root.getNodeName() + root.getNodeValue());
        DocumentImpl d = new DocumentImpl(t, 0);
        root = d.getDocumentElement();
        actual.append(uri);
        actual.append(root.getNodeName() + root.getNodeValue());
        
        Element clonedElem = (Element) root.cloneNode(false);
        clone.append(uri);
        clone.append(clonedElem.getNodeName() + clonedElem.getNodeValue());
        
        expected.append("\n");
        actual.append("\n");
        clone.append("\n");
    }
    System.out.println(expected.toString());
    System.out.println(actual.toString());
    System.out.println(clone.toString());
    assertEquals(actual.toString(), clone.toString());
    assertEquals(expected.toString(), clone.toString());
    }
}
