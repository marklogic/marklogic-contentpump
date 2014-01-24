package com.marklogic;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import com.marklogic.tree.NodeKind;

@RunWith(value = Parameterized.class)
public class TestDocumentImplClone extends AbstractTestCase {
    public static final Log LOG = LogFactory.getLog(TestDocumentImplClone.class);
	public TestDocumentImplClone(ForestData fd) {
        super(fd);
    }

    boolean verbose = false;

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
            if (t.rootNodeKind() != NodeKind.ELEM) continue;
            Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
            expected.append(t.getDocumentURI());
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
        	//text document failed in parsing as DOM
        	if (doc == null) continue;
            expected.append(uri);
            expected.append("#FIRSTCHILD##").
            		 append(doc.getFirstChild().getNodeName()).append("#").append("\n");
            expected.append("#LASTCHILD##").
            		 append(doc.getLastChild().getNodeName()).append("#").append("\n");
            
            Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
            actual.append(uri);
            String lname = d.getFirstChild().getNodeName();
            actual.append("#FIRSTCHILD##").append(lname).append("#").append("\n");
            lname = d.getLastChild().getNodeName();
            actual.append("#LASTCHILD##").append(lname).append("#").append("\n");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(expected.toString());
            LOG.debug(actual.toString());
        }
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
            //text document failed in parsing as DOM
            if (doc == null) continue;
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
            Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
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
        if (LOG.isDebugEnabled()) {
            LOG.debug(expected.toString());
            LOG.debug(actual.toString());
        }
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
            
            Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
            actual.append(d.getNodeName());
            actual.append("#OWNDERDOCROOT##").
   		 		   append(d.getOwnerDocument() == null).append("#");
            actual.append("#OWNDERDOCCHILD##").
   		 		   append(d.getFirstChild().getOwnerDocument().getNodeName()).append("#");
            
            String expectedUri = doc.getDocumentURI();
            String actualUri = d.getDocumentURI();
            
            if (!expectedUri.contains(actualUri)) {
                expected.append("#BASEURIROOT##").
       		 			 append(expectedUri).append("#");
                actual.append("#BASEURIROOT##").
		 			 append(actualUri).append("#");
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(expected.toString());
            LOG.debug(actual.toString());
        }
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
            Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
        	
            expected.append(d.getDocumentURI()).append("\n");
            actual.append(d.getDocumentURI()).append("\n");
            
            NodeList children = doc.getFirstChild().getChildNodes();
            for (int k = 0; k < children.getLength(); k++) {
              Node curr = children.item(k);
//              if(Utils.isWhitespaceNode(curr)) continue;
              expected.append("#NODENAME##").append(curr.getNodeName()).append("#").append("\n");
              String nodename = curr.getNodeName();
              int tok = nodename.indexOf(':'); 
              String prefix = (tok == -1)?null:nodename.substring(0, tok);
              String namespace = (tok == -1)?null:curr.lookupNamespaceURI(prefix);
              String localname = (tok == -1)?nodename:nodename.substring(tok+1);
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
        if (LOG.isDebugEnabled()) {
            LOG.debug(expected.toString());
            LOG.debug(actual.toString());
        }
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
        	for (int j = 0; j < esort.size(); j++) {
        		Node curr = esort.get(j);
        		expected.append("#I##").append(j).append("#").
        				append("#NODENAME##").append(curr.getNodeName()).append("#").
        				append("\n");
        	}
        		            
            Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
            firstelement = 0;
        	while (d.getChildNodes().item(firstelement).getNodeType() != Node.ELEMENT_NODE) firstelement++;
            Element rootActual = (Element) d.getChildNodes().item(firstelement);
        	NodeList elementsActual = rootActual.getElementsByTagName(tags[s]);
        	actual.append("#NUM##").append(elementsActual.getLength()).
			 	append("#").append("\n");
        	ArrayList<Node> asort = new ArrayList<Node>();
        	for (int j = 0; j < elementsActual.getLength(); j++) {
        		asort.add(elementsActual.item(j));
        	}
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
        if (LOG.isDebugEnabled()) {
            LOG.debug(expected.toString());
            LOG.debug(actual.toString());
        }
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
            	for (int j = 0; j < esort.size(); j++) {
            		Node curr = esort.get(j);
            		expected.append("#I##").append(j).append("#").
            				append("#NODENAME##").append(curr.getNodeName()).append("#").
            				append("\n");
            	}
            		            
                Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
                firstelement = 0;
            	while (d.getChildNodes().item(firstelement).getNodeType() != Node.ELEMENT_NODE) firstelement++;
                Element rootActual = (Element) d.getChildNodes().item(firstelement);
            	NodeList elementsActual = d.getElementsByTagName(tags[s]);
            	actual.append("#NUM##").append(elementsActual.getLength()).
    			 	append("#").append("\n");
            	ArrayList<Node> asort = new ArrayList<Node>();
            	for (int j = 0; j < elementsActual.getLength(); j++) {
            		asort.add(elementsActual.item(j));
            	}
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
        if (LOG.isDebugEnabled()) {
            LOG.debug(expected.toString());
            LOG.debug(actual.toString());
        }
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
            	for (int j = 0; j < esort.size(); j++) {
            		Node curr = esort.get(j);
            		expected.append("#I##").append(j).append("#").
            				append("#NODENAME##").append(curr.getNodeName()).append("#").
            				append("\n");
            	}
            		            
                Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
                firstelement = 0;
            	while (d.getChildNodes().item(firstelement).getNodeType() != Node.ELEMENT_NODE) firstelement++;
                Element rootActual = (Element) d.getChildNodes().item(firstelement);
            	NodeList elementsActual = rootActual.getElementsByTagNameNS(nss[n],tags[s]);
            	actual.append("#NUM##").append(elementsActual.getLength()).
    			 	append("#").append("\n");
            	ArrayList<Node> asort = new ArrayList<Node>();
            	for (int j = 0; j < elementsActual.getLength(); j++) {
            		asort.add(elementsActual.item(j));
            	}
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
        if (LOG.isDebugEnabled()) {
            LOG.debug(expected.toString());
            LOG.debug(actual.toString());
        }
        assertEquals(expected.toString(), actual.toString());
    }
    
    protected String getPostition(short comp) {
    	if (comp == 0x01) return "DOCUMENT_POSITION_DISCONNECTED";
    	if (comp == 0x02) return "DOCUMENT_POSITION_PRECEDING";
    	if (comp == 0x04) return "DOCUMENT_POSITION_FOLLOWING";
    	if (comp == 10) return "OCUMENT_POSITION_CONTAINS";
    	if (comp == 20) return "OCUMENT_POSITION_CONTAINED_BY";
    	if (comp == 0x20) return "DOCUMENT_POSITION_IMPLEMENTATION_SPECIFIC";
    	return "INVALID";
    }
    
    @Test
    public void testCompareDocumentPosition() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
                new File(testData + System.getProperty("file.separator")
                		+ forest, stand), false);
            assertEquals(num, trees.size());

            StringBuilder expected = new StringBuilder();
            StringBuilder actual = new StringBuilder();
            for (int i = 0; i < trees.size(); i++) {
                ExpandedTree t = trees.get(i);
                String uri = t.getDocumentURI();
           	
            	expected.append("#URI##").append(uri).append("#\n");
            	actual.append("#URI##").append(uri).append("#\n");
            	
                Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
            	if (doc == null) continue;
                NodeList children = doc.getChildNodes();
                if (children.getLength() >= 2) {
                	expected.append(getPostition(children.item(0).compareDocumentPosition(children.item(1)))).append("#\n");
                	expected.append(getPostition(children.item(1).compareDocumentPosition(children.item(0)))).append("#\n");
                }
                for (int j=0; j < children.getLength(); j++) {
                	if (children.item(j).hasChildNodes()) {
                		Node child = children.item(j).getChildNodes().item(0);
                		expected.append("#NODE1#").append(child.getNodeName());
                		expected.append("#NODE2#").append(children.item(0).getNodeName()).append("#\n");
                    	expected.append(getPostition(children.item(0).compareDocumentPosition(child))).append("#\n");
                    	expected.append(getPostition(child.compareDocumentPosition(children.item(0)))).append("#\n");
                	}
                }                 	
                
                Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
                children = d.getChildNodes();
                if (children.getLength() >= 2) {
                	actual.append(getPostition(children.item(0).compareDocumentPosition(children.item(1)))).append("#\n");
                	actual.append(getPostition(children.item(1).compareDocumentPosition(children.item(0)))).append("#\n");
                }
                for (int j=0; j < children.getLength(); j++) {
                	if (children.item(j).hasChildNodes()) {
                		Node child = children.item(j).getChildNodes().item(0);
                		actual.append("#NODE1#").append(child.getNodeName());
                		actual.append("#NODE2#").append(children.item(0).getNodeName()).append("#\n");;
                		actual.append(getPostition(children.item(0).compareDocumentPosition(child))).append("#\n");
                		actual.append(getPostition(child.compareDocumentPosition(children.item(0)))).append("#\n");
                	}
                }     
            	expected.append("\n");
            	actual.append("\n");
                
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(expected.toString());
                LOG.debug(actual.toString());
            }
            assertEquals(expected.toString(), actual.toString());
    }
    
    @Test
    public void testlookupNamespaceURIPrefix() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
                new File(testData + System.getProperty("file.separator")
                		+ forest, stand), false);
            assertEquals(num, trees.size());

            StringBuffer expected = new StringBuffer();
            StringBuffer actual = new StringBuffer();
            
            String pres[] = {"html","xsi","emp","dc","f","h","stk"};
            String nss[] = {"http://www.w3.org/TR/html4/"};

            for (int i = 0; i < trees.size(); i++) {
                ExpandedTree t = trees.get(i);
            	String uri = t.getDocumentURI();
            	
            	Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
                //text document failed in parsing as DOM
                if (doc == null) continue;        	
                expected.append("\n").append(uri).append("\n");
            	Queue<NodeList> q = new LinkedList<NodeList>();
            	if (doc.hasChildNodes()) q.add(doc.getChildNodes());
            	while (!q.isEmpty()) {
            		NodeList nl = q.poll();
            		for (int k=0; k<nl.getLength(); k++) {
            			if (nl.item(k).hasChildNodes()) 
            				q.add(nl.item(k).getChildNodes());
            			if (nl.item(k).getNodeType() == Node.TEXT_NODE) continue;
            			if ("cdata-section".equals(nl.item(k).getNodeName())) continue;
                        expected.append("#NODE##").
             		     append(nl.item(k).getNodeName()).append("#").append("\n");
                        for (int p=0; p<pres.length; p++) {
                        	String nslookup = nl.item(k).lookupNamespaceURI(pres[p]);
                        	if (null == nslookup) continue;
                        	expected.append("#PREFIX#").append(pres[p]).append("#NS#").
           						append(nslookup).append("\n");
                        }
                        for (int p=0; p<nss.length; p++) {
                        	String nslookup = nl.item(k).lookupPrefix(nss[p]);
                        	if (null == nslookup) continue;
                        	expected.append("#NS#").append(pres[p]).append("#PREFIX#").
           						append(nslookup).append("\n");
                        }
            		}
            	}
            	
                q.clear();
                Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
                actual.append("\n").append(uri).append("\n");
            	if (d.hasChildNodes()) q.add(d.getChildNodes());
            	while (!q.isEmpty()) {
            		NodeList nl = q.poll();
            		for (int k=0; k<nl.getLength(); k++) {
            			if (nl.item(k).hasChildNodes()) 
            				q.add(nl.item(k).getChildNodes());
            			if (nl.item(k).getNodeType() == Node.TEXT_NODE) continue;
            			actual.append("#NODE##").
             		     append(nl.item(k).getNodeName()).append("#").append("\n");
                        for (int p=0; p<pres.length; p++) {
                        	String nslookup = nl.item(k).lookupNamespaceURI(pres[p]);
                        	if (null == nslookup) continue;
                        	actual.append("#PREFIX#").append(pres[p]).append("#NS#").
           						append(nslookup).append("\n");
                        }
                        for (int p=0; p<nss.length; p++) {
                        	String nslookup = nl.item(k).lookupPrefix(nss[p]);
                        	if (null == nslookup) continue;
                        	actual.append("#NS#").append(pres[p]).append("#PREFIX#").
           						append(nslookup).append("\n");
                        }
            		}
            		
            	}
                
            	expected.append("#");
            	actual.append("#");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(expected.toString());
                LOG.debug(actual.toString());
            }
            assertEquals(expected.toString(), actual.toString());
    }
    
    @Test
    public void testGetHasAttribute() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
                new File(testData + System.getProperty("file.separator")
                		+ forest, stand), false);
            assertEquals(num, trees.size());

            StringBuffer expected = new StringBuffer();
            StringBuffer actual = new StringBuffer();
            
            String attrs[] = {"xmlns","onload","base","x","value","union","id"};

            for (int i = 0; i < trees.size(); i++) {
                ExpandedTree t = trees.get(i);
            	String uri = t.getDocumentURI();
            	
            	Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
                //text document failed in parsing as DOM
                if (doc == null) continue;	
                expected.append("\n").append(uri).append("\n");
            	Queue<NodeList> q = new LinkedList<NodeList>();
            	if (doc.hasChildNodes()) q.add(doc.getChildNodes());
            	while (!q.isEmpty()) {
            		NodeList nl = q.poll();
            		for (int k=0; k<nl.getLength(); k++) {
            			if (nl.item(k).hasChildNodes()) 
            				q.add(nl.item(k).getChildNodes());
            			if (nl.item(k).getNodeType() != Node.ELEMENT_NODE) continue;
            			if ("cdata-section".equals(nl.item(k).getNodeName())) continue;
                        expected.append("#NODE##").
             		     append(nl.item(k).getNodeName()).append("#").append("\n");
                        for (int p=0; p<attrs.length; p++) {
                        	Element elem = (Element)nl.item(k);
                        	String value = elem.getAttribute(attrs[p]);
                        	expected.append("#ATTR#").append(attrs[p]).
                        		append("#VALUE#").append(value).append("\n");
                        }
            		}
            	}
            	
                q.clear();
                Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
                actual.append("\n").append(uri).append("\n");
            	if (d.hasChildNodes()) q.add(d.getChildNodes());
            	while (!q.isEmpty()) {
            		NodeList nl = q.poll();
            		for (int k=0; k<nl.getLength(); k++) {
            			if (nl.item(k).hasChildNodes()) 
            				q.add(nl.item(k).getChildNodes());
            			if (nl.item(k).getNodeType() != Node.ELEMENT_NODE) continue;
            			if ("cdata-section".equals(nl.item(k).getNodeName())) continue;
            			actual.append("#NODE##").
             		     append(nl.item(k).getNodeName()).append("#").append("\n");
                        for (int p=0; p<attrs.length; p++) {
                        	Element elem = (Element)nl.item(k);
                        	String value = elem.getAttribute(attrs[p]);
                        	actual.append("#ATTR#").append(attrs[p]).
                        		append("#VALUE#").append(value).append("\n");
                        }
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
            Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
            if (doc == null) continue;
            expected.append(uri);
            NodeList children = doc.getChildNodes();
            walkDOM(children, expected);
            if (LOG.isDebugEnabled()) 
                LOG.debug("--------------");
            Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
            NodeList eChildren = d.getChildNodes();
            actual.append(uri);
            walkDOM (eChildren, actual);
            
            expected.append("\n");
            actual.append("\n");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(expected.toString());
            LOG.debug(actual.toString());
        }
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
            Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        	if (doc == null) continue;
        	expected.append(uri);
        	NodeList children = doc.getChildNodes();
            walkDOMNextSibling(children, expected);
            Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
            NodeList eChildren = d.getChildNodes();
            actual.append(uri);
            walkDOMNextSibling (eChildren, actual);
            
            expected.append("\n");
            actual.append("\n");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(expected.toString());
            LOG.debug(actual.toString());
        }
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
            
            Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        	if (doc == null) continue;
        	expected.append(uri);
            NodeList children = doc.getChildNodes();
            walkDOMPreviousSibling(children, expected);
            Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
            NodeList eChildren = d.getChildNodes();
            actual.append(uri);
            walkDOMPreviousSibling (eChildren, actual);
            
            expected.append("\n");
            actual.append("\n");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(expected.toString());
            LOG.debug(actual.toString());
        }
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
            
            Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        	if (doc == null) continue;
        	expected.append(uri);
            NodeList children = doc.getChildNodes();
            walkDOMParent(children, expected);
            Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
            NodeList eChildren = d.getChildNodes();
            actual.append(uri);
            walkDOMParent (eChildren, actual);
            
            expected.append("\n");
            actual.append("\n");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(expected.toString());
            LOG.debug(actual.toString());
        }
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
            
            Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        	if (doc == null) continue;
        	expected.append(uri);
            NodeList children = doc.getChildNodes();
            walkDOMTextContent(children, expected);
            Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
            NodeList eChildren = d.getChildNodes();
            actual.append(uri);
            walkDOMTextContent (eChildren, actual);
            
            expected.append("\n");
            actual.append("\n");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(expected.toString());
            LOG.debug(actual.toString());
        }
        assertEquals(expected.toString(), actual.toString());
    }
    

    @Test
    public void testAttributes() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(
                new File(testData + System.getProperty("file.separator")
                		+ forest, stand), false);
            assertEquals(num, trees.size());

        Set<String> expectedAttrSet = new HashSet<String>();
        Set<String> actualAttrSet = new HashSet<String>();
        for (int i = 0; i < trees.size(); i++) {
            ExpandedTree t = trees.get(i);
            String uri = t.getDocumentURI();
            expectedAttrSet.add(uri);
            actualAttrSet.add(uri);
            Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
        	if (doc == null) continue;
            NodeList children = doc.getChildNodes();
            walkDOMAttr(children, expectedAttrSet);
            Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
            NodeList eChildren = d.getChildNodes();
            walkDOMAttr (eChildren, actualAttrSet);
        }
        for(String s : expectedAttrSet) {
            if(actualAttrSet.contains(s) == false && expectedMissingNSDecl.contains(s) == false) {
                if (LOG.isDebugEnabled()) 
                    LOG.debug("NOT_FOUND:" + s);
                assertTrue(actualAttrSet.contains(s));
            }
        }

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
            Document doc = Utils.readXMLasDOMDocument(new File(testData, uri));
            if (doc == null) continue;
            expected.append(uri);
            NodeList children = doc.getChildNodes();
            walkDOMElem(children, expected);
            Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
            NodeList eChildren = d.getChildNodes();
            actual.append(uri);
            walkDOMElem(eChildren, actual);
            
            expected.append("\n");
            actual.append("\n");
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(expected.toString());
            LOG.debug(actual.toString());
        }
        assertEquals(expected.toString(), actual.toString());
    }
    
    
    @Test
    public void testIsDefaultNS() throws IOException {
        List<ExpandedTree> trees = Utils.decodeTreeData(new File(testData
            + System.getProperty("file.separator") + forest, stand), false);
        assertEquals(num, trees.size());

        StringBuilder expected = new StringBuilder();
        StringBuilder actual = new StringBuilder();
        String ns[] = { "http://www.w3.org/TR/html4/", "urn:loc.gov:books" };
        for (int ni = 0; ni < ns.length; ni++) {
            for (int i = 0; i < trees.size(); i++) {
                ExpandedTree t = trees.get(i);
                String uri = t.getDocumentURI();
                expected.append(uri).append("#");
                expected.append(ns[ni]).append("@isDefaultNS:");
                Document doc = Utils.readXMLasDOMDocument(new File(testData,
                    uri));
                if (doc == null)
                    continue;
                expected.append(doc.isDefaultNamespace(ns[ni])).append("\n");

                Document d = (Document) new DocumentImpl(t, 0).cloneNode(true);
                actual.append(uri).append("#");
                actual.append(ns[ni]).append("@isDefaultNS:");
                actual.append(d.isDefaultNamespace(ns[ni])).append("\n");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(expected);
                LOG.debug("\n");
                LOG.debug(actual);
                LOG.debug("\n");
            }
        }

    }
}
