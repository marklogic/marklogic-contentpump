package com.marklogic.dom;

import com.marklogic.tree.ExpandedTree;
import org.junit.Test;

import static org.junit.Assert.*;

public class NodeImplTest {

  @Test
  public void isSupportedCore() {
    NodeImpl node = new TestNodeImpl();
    assertTrue(node.isSupported("Core", "1.0"));
    assertTrue(node.isSupported("core", null));
    assertTrue(node.isSupported("CORE", "99"));
  }

  public void isSupportedXml() {
    NodeImpl node = new TestNodeImpl();
    assertTrue(node.isSupported("XML", "1.0"));
  }

  public void isSupportedInvalidFeature() {
    NodeImpl node = new TestNodeImpl();
    assertFalse(node.isSupported("InvalidFeature", "1.0"));
  }

  static class TestNodeImpl extends NodeImpl {

    TestNodeImpl() {
      super(new ExpandedTree(), 0);
    }

    public String getNodeName() {
      throw new UnsupportedOperationException();
    }
  }

}