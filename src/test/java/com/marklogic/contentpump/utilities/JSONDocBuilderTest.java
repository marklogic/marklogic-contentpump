package com.marklogic.contentpump.utilities;

import com.fasterxml.jackson.core.JsonGenerator;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class JSONDocBuilderTest {

  @Test
  public void newDoc() {
    JSONDocBuilder jsonDocBuilder = new JSONDocBuilder();
    jsonDocBuilder.init(null);
    try {
      jsonDocBuilder.newDoc();
      assertNotNull(jsonDocBuilder.generator);
      assertTrue(jsonDocBuilder.generator instanceof JsonGenerator);
    } catch (IOException ex) {
      fail();
    }
  }
}