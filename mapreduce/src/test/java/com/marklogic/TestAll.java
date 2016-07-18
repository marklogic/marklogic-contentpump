package com.marklogic;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({ TestDocumentImpl.class, TestDocumentImplClone.class })
public class TestAll {

}
