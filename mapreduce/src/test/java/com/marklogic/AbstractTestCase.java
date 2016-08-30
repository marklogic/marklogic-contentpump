package com.marklogic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

@RunWith(value = Parameterized.class)
public class AbstractTestCase extends TestCase {
    public static final Log LOG = LogFactory.getLog(AbstractTestCase.class);
    String testData;
    String forest;
    String stand;
    int num;
    static Set<String> expectedMissingNSDecl = new HashSet<>();
    static {
        expectedMissingNSDecl.add("acronym@xmlns=#parent is null#nextSibling is null");
    }

    AbstractTestCase(ForestData fd) {
        testData = fd.getPath();
        forest = fd.getName();
        stand = fd.getStand();
        num = fd.getNumDocs();
    }

    // requires to return a collection of arrays
    @Parameters
    public static Collection data() {
        List<ForestData[]> dataList = new ArrayList<>();
        ForestData[] ds1 = { new ForestData("src/test/resources/ns-prefix",
            "ns-prefix-forest", "00000001", 23) };
        dataList.add(ds1);

        ForestData[] ds2 = { new ForestData("src/test/resources/3doc-test",
            "3docForest", "00000002", 4) };
        dataList.add(ds2);

        ForestData[] ds3 = { new ForestData("src/test/resources/dom-core-test",
            "DOM-test-forest", "00000002", 16) };
        dataList.add(ds3);

        return Arrays.asList(dataList.toArray());

    }

    protected void walkDOM(NodeList nodes, StringBuilder sb) {
        boolean seenText = false;
        for (int i = 0; i < nodes.getLength(); i++) {
            Node child = nodes.item(i);
            if (Utils.isWhitespaceNode(child)) { continue; }
            
            if (child.getNodeType() == Node.TEXT_NODE) { seenText = true; }
            if ("#cdata-section".equals(child.getNodeName())) {
                if (seenText) {
                    //remove the \n from immediate previous text node
                    sb.setLength(sb.length() -1 );
                } else {
                    sb.append(child.getNodeType()-1).append("#");
                    //no text, cdata itself becomes text
                    sb.append("#text").append("#");
                }
                sb.append(child.getNodeValue().replaceAll("\\s+", "")).append("#");
            } else {
                sb.append(child.getNodeType()).append("#");
                sb.append(child.getNodeName()).append("#");
                sb.append(child.getNodeValue()==null?null:child.getNodeValue().replaceAll("\\s+", "")).append("#");
            }

            if (child.hasChildNodes()) {
                sb.append("\n");
                walkDOM(child.getChildNodes(), sb);
            }
        }
    }

    protected void walkDOMAttr(NodeList nodes, StringBuilder sb) {
        for (int i = 0; i < nodes.getLength(); i++) {
            Node n = nodes.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                if (LOG.isDebugEnabled())
                    LOG.debug(n.getNodeName());
            }
            if (n.hasAttributes()) {
                List<String> list = new ArrayList<>();
                sb.append(n.getNodeName()).append("#\n");
                NamedNodeMap nnMap = n.getAttributes();
                for (int j = 0; j < nnMap.getLength(); j++) {
                    Attr attr = (Attr) nnMap.item(j);
                    String tmp = "@" + attr.getName() + "=" + attr.getValue();
                    list.add(tmp);
                    list.add("#isSpecified:" + attr.getSpecified() + "\n");
                }
                Collections.sort(list);
                sb.append(list.toString());
            }
            sb.append("\n");
            if (n.hasChildNodes()) {
                walkDOMAttr(n.getChildNodes(), sb);
            }
        }
    }

    protected void walkDOMAttr(NodeList nodes, Set<String> sb) {
        for (int i = 0; i < nodes.getLength(); i++) {
            Node n = nodes.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                if (LOG.isDebugEnabled())
                    LOG.debug(n.getNodeName());
            }
            if (n.hasAttributes()) {
                NamedNodeMap nnMap = n.getAttributes();
                for (int j = 0; j < nnMap.getLength(); j++) {
                    Attr attr = (Attr) nnMap.item(j);
                    String tmp = n.getNodeName() + "@" + attr.getName() + "="
                        + attr.getValue().replaceAll("\\s+", "");
                    tmp += "#parent is " + attr.getParentNode();
                    tmp += "#nextSibling is " + attr.getNextSibling();
                    if (LOG.isDebugEnabled())
                        LOG.debug(tmp);
                    sb.add(tmp);
                }
            }
            if (n.hasChildNodes()) {
                walkDOMAttr(n.getChildNodes(), sb);
            }
        }
    }

    protected void walkDOMElem(NodeList nodes, StringBuilder sb) {
        for (int i = 0; i < nodes.getLength(); i++) {
            Node n = nodes.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                sb.append(n.getNodeName()).append("#");
                Attr attr = ((Element) n).getAttributeNode("id");
                if (attr != null) {
                    sb.append("@").append(attr.getName()).append("=")
                        .append(attr.getValue().replaceAll("\\s+", ""));
                    sb.append("@id=").append(
                        ((Element) n).getAttribute("id")
                            .replaceAll("\\s+", ""));
                    sb.append("#isSpecified:").append(attr.getSpecified());
                }

            } else if (Utils.isWhitespaceNode(n)) {
                continue;
            } else {
                sb.append(n.getNodeValue().trim());
            }
            sb.append("\n");
            if (n.hasChildNodes()) {
                walkDOMElem(n.getChildNodes(), sb);
            }
        }
    }

    protected void walkDOMNextSibling(NodeList nodes, StringBuilder sb) {
        if (nodes.getLength() <= 0)
            return;
        boolean seenText = false;
        Node child = nodes.item(0);
        while (child != null) {
            if (Utils.isWhitespaceNode(child)) {
                child = child.getNextSibling();
                continue;
            }
            if (child.getNodeType() == Node.TEXT_NODE) { seenText = true; }
            if ("#cdata-section".equals(child.getNodeName())) {
                if (seenText) {
                    //remove the \n from immediate previous text node
                    sb.setLength(sb.length() -1 );
                } else {
                    sb.append(child.getNodeType()-1).append("#");
                    //no text, cdata itself becomes text
                    sb.append("#text").append("#");
                }
                sb.append(child.getNodeValue().replaceAll("\\s+", ""));
            } else {
                sb.append(child.getNodeType()).append("#");
                sb.append(child.getNodeName()).append("#");
                sb.append(child.getNodeValue()==null? null:child.getNodeValue().replaceAll("\\s+", ""));
                
            }
            sb.append("\n");
            
            walkDOMNextSibling(child.getChildNodes(), sb);
            // next sibling
            child = child.getNextSibling();
        }
    }

    protected void walkDOMParent(NodeList nodes, StringBuilder sb) {
        boolean firstSeenCDATA = false;
        boolean seenText = false;
        for (int i = 0; i < nodes.getLength(); i++) {
            Node child = nodes.item(i);
            if (Utils.isWhitespaceNode(child)) {
                continue;
            }
            if (child.getNodeType() == Node.TEXT_NODE) { seenText = true; }
            if ("#cdata-section".equals(child.getNodeName())) {
                if(firstSeenCDATA == false) {
                    firstSeenCDATA = true;
                    if(seenText) { continue; }
                    //no text, so cdata itself will become text in xdm
                    sb.append(child.getNodeType() - 1).append("#");
                    sb.append("#text").append("'s parent is ");
                    sb.append(child.getParentNode().getNodeName()).append("#");
                } else {
                    continue;
                }
            } else {
                sb.append(child.getNodeType()).append("#");
                sb.append(child.getNodeName()).append("'s parent is ");
                sb.append(child.getParentNode().getNodeName()).append("#");
            }
            sb.append("\n");
            
            if (child.hasChildNodes()) {
                walkDOMParent(child.getChildNodes(), sb);
            }
        }
    }

    protected void walkDOMPreviousSibling(NodeList nodes, StringBuilder sb) {
        if (nodes.getLength() <= 0) {
            return;
        }
        Node child = nodes.item(nodes.getLength() - 1);
        while (child != null) {
            if (Utils.isWhitespaceNode(child)) {
                child = child.getPreviousSibling();
                continue;
            }
            if ("#cdata-section".equals(child.getNodeName())) {
                sb.append(child.getNodeType() -1).append("#");
                sb.append("#text").append("#");
            } else {
                sb.append(child.getNodeType()).append("#");
                sb.append(child.getNodeName()).append("#");
            }

            sb.append(child.getNodeValue()).append("#");
            sb.append("\n");
            walkDOMPreviousSibling(child.getChildNodes(), sb);
            // next sibling
            child = child.getPreviousSibling();
        }
    }

    protected void walkDOMTextContent(NodeList nodes, StringBuilder sb) {
        boolean seenText = false;
        for (int i = 0; i < nodes.getLength(); i++) {
            Node n = nodes.item(i);
            if (Utils.isWhitespaceNode(n)) {
                continue;
            }
            if (n.getNodeType() == Node.TEXT_NODE) { seenText = true; }
            if ("#cdata-section".equals(n.getNodeName())) {
                if (seenText) {
                    //remove the # from immediate previous text node
                    sb.setLength(sb.length() -1 );
                } else {
                    sb.append(n.getNodeType()-1).append("#");
                }
                sb.append(n.getTextContent().replaceAll("\\s+", ""));
                
            } else {
                sb.append(n.getNodeType()).append("#");
                sb.append(n.getTextContent().replaceAll("\\s+", ""));
            }
            sb.append("\n");
            
            if (n.hasChildNodes()) {
                walkDOMTextContent(n.getChildNodes(), sb);
            }
        }
    }
}
