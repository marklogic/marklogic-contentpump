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
package com.marklogic.contentpump;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Test;

import com.marklogic.contentpump.utilities.OptionsFileUtil;
import com.marklogic.xcc.ResultSequence;

/**
 * @author mattsun
 *
 */
public class testDistributedImportDelimitedJson {

    @After
    public void tearDown() {
        Utils.closeSession();
    }
    
    @Test
    public void testImportDelimitedJSON() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/delimitedJson/sample1.txt"
                + " -uri_id name -generate_uri false"
                + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR + " -mode distributed"
                + " -input_file_type delimited_json";
        
        String[] args = cmd.split(" ");
        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");
        
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        Utils.prepareDistributedMode();
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery("xcc://admin:admin@localhost:5275",
                "fn:count(fn:doc())");
        assertTrue(result.hasNext());
        assertEquals("3", result.next().asString());
        
        Utils.closeSession();
        
        result = Utils.assertDocsFormat("xcc://admin:admin@localhost:5275","JSON");
        assertTrue(result.hasNext());
        assertTrue(result.next().asString().equals("true"));
        
        Utils.closeSession();
        
        result = Utils.getAllDocs("xcc://admin:admin@localhost:5275");
        StringBuilder sb = new StringBuilder();
        while (result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        
        String key = Utils
                .readSmallFile(Constants.TEST_PATH.toUri().getPath()
                        + "/keys/TestImportDelimitedJson#testImportDelimitedJson.txt");
        assertTrue(sb.toString().equals(key));
    }
    
    @Test
    public void testImportDelimitedJSONDir() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/delimitedJson"
                + " -uri_id name -generate_uri false -input_file_pattern sample.*\\.txt"
                + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR + " -mode distributed"
                + " -input_file_type delimited_json";
        
        String[] args = cmd.split(" ");
        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");
        
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        Utils.prepareDistributedMode();
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery("xcc://admin:admin@localhost:5275",
                "fn:count(fn:doc())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        
        Utils.closeSession();
        
        result = Utils.assertDocsFormat("xcc://admin:admin@localhost:5275","JSON");
        assertTrue(result.hasNext());
        assertTrue(result.next().asString().equals("true"));
        
        Utils.closeSession();
        
        result = Utils.getAllDocs("xcc://admin:admin@localhost:5275");
        StringBuilder sb = new StringBuilder();
        while (result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        
        String key = Utils
                .readSmallFile(Constants.TEST_PATH.toUri().getPath()
                        + "/keys/TestImportDelimitedJson#testImportDelimitedJsonDir.txt");
        assertTrue(sb.toString().equals(key));
    }
    
    @Test
    public void testImportDelimitedJSONGenUri() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/delimitedJson"
                + " -generate_uri true -input_file_pattern sample.*\\.txt"
                + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR + " -mode distributed"
                + " -input_file_type delimited_json";
        
        String[] args = cmd.split(" ");
        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");
        
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        Utils.prepareDistributedMode();
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery("xcc://admin:admin@localhost:5275",
                "fn:count(fn:doc())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        
        Utils.closeSession();
        
        result = Utils.assertDocsFormat("xcc://admin:admin@localhost:5275","JSON");
        assertTrue(result.hasNext());
        assertTrue(result.next().asString().equals("true"));
        
        Utils.closeSession();
        
        result = Utils.getNonEmptyDocsURIs("xcc://admin:admin@localhost:5275");
        String keys[] = {"src/test/resources/delimitedJson/sample1.txt-0-1</uri>",
                "src/test/resources/delimitedJson/sample1.txt-0-2</uri>",
                "src/test/resources/delimitedJson/sample1.txt-0-3</uri>",
                "src/test/resources/delimitedJson/sample2.txt-0-1</uri>",
                "src/test/resources/delimitedJson/sample2.txt-0-2</uri>"};
        int counter = 0;
        while (result.hasNext()) {
            String s = result.next().asString();
            // The string is in format <uri>uri_</uri> format
            assertTrue(s.endsWith(keys[counter++]));
        }
    }
    
    @Test
    public void testImportDelimitedJSONGenTestUri() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/delimitedJson/testUri.txt"
                + " -uri_id name -generate_uri false"
                + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR + " -mode distributed"
                + " -input_file_type delimited_json";
        
        String[] args = cmd.split(" ");
        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");
        
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        Utils.prepareDistributedMode();
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery("xcc://admin:admin@localhost:5275",
                "fn:count(fn:doc())");
        assertTrue(result.hasNext());
        assertEquals("1", result.next().asString());
        
        Utils.closeSession();
        
        result = Utils.getNonEmptyDocsURIs("xcc://admin:admin@localhost:5275");
        String key = "<uri>rose</uri>";
        assertTrue(result.hasNext());
        assertTrue(result.next().asString().equals(key));
        
    }
    
    @Test
    public void testImportDelimitedJSONZip() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/delimitedJson/zip/sample.zip"
                + " -uri_id name -generate_uri false -input_compressed true"
                + " -input_compression_codec zip"
                + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR + " -mode distributed"
                + " -input_file_type delimited_json";
        
        String[] args = cmd.split(" ");
        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");
        
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        Utils.prepareDistributedMode();
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery("xcc://admin:admin@localhost:5275",
                "fn:count(fn:doc())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        
        Utils.closeSession();
        
        result = Utils.assertDocsFormat("xcc://admin:admin@localhost:5275","JSON");
        assertTrue(result.hasNext());
        assertTrue(result.next().asString().equals("true"));
        
        Utils.closeSession();
        
        result = Utils.getAllDocs("xcc://admin:admin@localhost:5275");
        StringBuilder sb = new StringBuilder();
        while (result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        
        String key = Utils
                .readSmallFile(Constants.TEST_PATH.toUri().getPath()
                        + "/keys/TestImportDelimitedJson#testImportDelimitedJsonDir.txt");
        assertTrue(sb.toString().equals(key));
    }
    
    @Test
    public void testImportDelimitedJSONZipDir() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/delimitedJson/zip"
                + " -uri_id name -generate_uri false -input_compressed true"
                + " -input_compression_codec zip"
                + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR + " -mode distributed"
                + " -input_file_type delimited_json";
        
        String[] args = cmd.split(" ");
        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");
        
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        Utils.prepareDistributedMode();
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery("xcc://admin:admin@localhost:5275",
                "fn:count(fn:doc())");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());
        
        Utils.closeSession();
        
        result = Utils.assertDocsFormat("xcc://admin:admin@localhost:5275","JSON");
        assertTrue(result.hasNext());
        assertTrue(result.next().asString().equals("true"));
        
        Utils.closeSession();
        
        result = Utils.getAllDocs("xcc://admin:admin@localhost:5275");
        StringBuilder sb = new StringBuilder();
        while (result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        
        String key = Utils
                .readSmallFile(Constants.TEST_PATH.toUri().getPath()
                        + "/keys/TestImportDelimitedJson#testImportDelimitedJSONZipDir.txt");
        assertTrue(sb.toString().equals(key));
    }
    
    @Test
    public void testImportDelimitedJSONGzip() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/delimitedJson/gzip/sample.gz"
                + " -uri_id name -generate_uri false -input_compressed true"
                + " -input_compression_codec gzip"
                + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR + " -mode distributed"
                + " -input_file_type delimited_json";
        
        String[] args = cmd.split(" ");
        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");
        
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        Utils.prepareDistributedMode();
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery("xcc://admin:admin@localhost:5275",
                "fn:count(fn:doc())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        
        Utils.closeSession();
        
        result = Utils.assertDocsFormat("xcc://admin:admin@localhost:5275","JSON");
        assertTrue(result.hasNext());
        assertTrue(result.next().asString().equals("true"));
        
        Utils.closeSession();
        
        result = Utils.getAllDocs("xcc://admin:admin@localhost:5275");
        StringBuilder sb = new StringBuilder();
        while (result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        
        String key = Utils
                .readSmallFile(Constants.TEST_PATH.toUri().getPath()
                        + "/keys/TestImportDelimitedJson#testImportDelimitedJsonDir.txt");
        assertTrue(sb.toString().equals(key));
    }
    
    @Test
    public void testImportDelimitedJSONGzipDir() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/delimitedJson/gzip"
                + " -uri_id name -generate_uri false -input_compressed true"
                + " -input_compression_codec gzip"
                + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR + " -mode distributed"
                + " -input_file_type delimited_json";
        
        String[] args = cmd.split(" ");
        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");
        
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        Utils.prepareDistributedMode();
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery("xcc://admin:admin@localhost:5275",
                "fn:count(fn:doc())");
        assertTrue(result.hasNext());
        assertEquals("6", result.next().asString());
        
        Utils.closeSession();
        
        result = Utils.assertDocsFormat("xcc://admin:admin@localhost:5275","JSON");
        assertTrue(result.hasNext());
        assertTrue(result.next().asString().equals("true"));
        
        Utils.closeSession();
        
        result = Utils.getAllDocs("xcc://admin:admin@localhost:5275");
        StringBuilder sb = new StringBuilder();
        while (result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        
        String key = Utils
                .readSmallFile(Constants.TEST_PATH.toUri().getPath()
                        + "/keys/TestImportDelimitedJson#testImportDelimitedJSONZipDir.txt");
        assertTrue(sb.toString().equals(key));
    }
    
    @Test
    public void testImportDelimitedJSONZipFnCollection() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/delimitedJson/zip/sample.zip"
                + " -uri_id name -generate_uri false -input_compressed true -input_compression_codec zip"
                + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR + " -mode distributed"
                + " -input_file_type delimited_json -filename_as_collection true";
        
        String[] args = cmd.split(" ");
        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");
        
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        Utils.prepareDistributedMode();
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery("xcc://admin:admin@localhost:5275",
                "fn:count(fn:doc())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        
        Utils.closeSession();
        
        result = Utils.assertDocsFormat("xcc://admin:admin@localhost:5275","JSON");
        assertTrue(result.hasNext());
        assertTrue(result.next().asString().equals("true"));
        
        Utils.closeSession();
        
        result = Utils.runQuery("xcc://admin:admin@localhost:5275","fn:collection(\"sample.zip_sample1.txt\");");
        StringBuilder sb = new StringBuilder();
        while (result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        
        String key = Utils
                .readSmallFile(Constants.TEST_PATH.toUri().getPath()
                        + "/keys/TestImportDelimitedJson#testImportDelimitedJsonZipFnCollection1.txt");
        assertTrue(sb.toString().equals(key));
        
        Utils.closeSession();
        
        result = Utils.runQuery("xcc://admin:admin@localhost:5275","fn:collection(\"sample.zip_sample2.txt\");");
        sb = new StringBuilder();
        while (result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        
        key = Utils
                .readSmallFile(Constants.TEST_PATH.toUri().getPath()
                        + "/keys/TestImportDelimitedJson#testImportDelimitedJsonZipFnCollection2.txt");
        assertTrue(sb.toString().equals(key));
    }
    
    @Test
    public void testImportDelimitedJSONGzipFnCollection() throws Exception {
        String cmd = "IMPORT -host localhost -port 5275 -username admin -password admin"
                + " -input_file_path " + Constants.TEST_PATH.toUri() + "/delimitedJson/gzip/sample.gz"
                + " -uri_id name -generate_uri false -input_compressed true -input_compression_codec gzip"
                + " -hadoop_conf_dir " + Constants.HADOOP_CONF_DIR + " -mode distributed"
                + " -input_file_type delimited_json -filename_as_collection true";
        
        String[] args = cmd.split(" ");
        Utils.clearDB("xcc://admin:admin@localhost:5275", "Documents");
        
        String[] expandedArgs = null;
        expandedArgs = OptionsFileUtil.expandArguments(args);
        Utils.prepareDistributedMode();
        ContentPump.runCommand(expandedArgs);
        
        ResultSequence result = Utils.runQuery("xcc://admin:admin@localhost:5275",
                "fn:count(fn:doc())");
        assertTrue(result.hasNext());
        assertEquals("5", result.next().asString());
        
        Utils.closeSession();
        
        result = Utils.assertDocsFormat("xcc://admin:admin@localhost:5275","JSON");
        assertTrue(result.hasNext());
        assertTrue(result.next().asString().equals("true"));
        
        Utils.closeSession();
        
        result = Utils.runQuery("xcc://admin:admin@localhost:5275","fn:collection(\"sample.gz\");");
        StringBuilder sb = new StringBuilder();
        while (result.hasNext()) {
            String s = result.next().asString();
            sb.append(s);
        }
        
        String key = Utils
                .readSmallFile(Constants.TEST_PATH.toUri().getPath()
                        + "/keys/TestImportDelimitedJson#testImportDelimitedJsonGZipFnCollection.txt");
        assertTrue(sb.toString().equals(key));
    }
}
