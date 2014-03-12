package com.marklogic.contentpump;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Properties;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;

public class Utils {
    private static HashMap<String, ContentSource> csMap = new HashMap<String, ContentSource>();
    private static Session session;
    public static String newLine = System.getProperty("line.separator");
    public static boolean moduleReady = false;
    public static void prepareDistributedMode() {
        Properties props = System.getProperties();
        props.setProperty(ConfigConstants.CONTENTPUMP_HOME_PROPERTY_NAME,
            Constants.CONTENTPUMP_HOME);
        System.setProperties(props);
    }
    
    public static ResultSequence runQuery(String xccUri, String query)
        throws XccConfigException, URISyntaxException, RequestException {
        ContentSource cs = csMap.get(xccUri);
        if (cs == null) {
            cs = ContentSourceFactory.newContentSource(new URI(
            xccUri));
            csMap.put(xccUri, cs);
        }
        session = cs.newSession();
        AdhocQuery aquery = session.newAdhocQuery(query);

        RequestOptions options = new RequestOptions();
        options.setCacheResult(false);
        options.setDefaultXQueryVersion("1.0-ml");
        aquery.setOptions(options);
        return session.submitRequest(aquery);
    }
    
    public static void prepareModule(String xccUri, String moduleUri)
        throws XccConfigException, RequestException, URISyntaxException {
        if (moduleReady)
            return;
        String query = "xquery version \"1.0-ml\";\n"
            + "xdmp:eval('xdmp:document-load(\""
            + Constants.TEST_PATH
            + moduleUri + "\", <options xmlns=\"xdmp:document-load\">"
            + "<uri>" + moduleUri + "</uri></options>)',(),<options xmlns=\"xdmp:eval\">"
            + "<database>{xdmp:database-forests(xdmp:database(\"Modules\"))}</database></options>)";
        runQuery(xccUri, query);
    }
    

    public static void setDirectoryCreation(String xccUri, String mode)
        throws XccConfigException, RequestException, URISyntaxException {
        String query = "xquery version \"1.0-ml\"\n;"
            + "import module namespace admin = \"http://marklogic.com/xdmp/admin\" at \"/MarkLogic/admin.xqy\";\n"
            + "let $config := admin:get-configuration()\n"
            + "return admin:database-set-directory-creation($config, xdmp:database(), \""
            + mode + "\")";
        runQuery(xccUri, query);
    }

    public static void clearDB(String xccUri, String dbName)
        throws XccConfigException, RequestException, URISyntaxException, 
        InterruptedException {
        String q = "xquery version \"1.0-ml\"\n;"
            + "import module namespace admin = \"http://marklogic.com/xdmp/admin\" at \"/MarkLogic/admin.xqy\";\n"
//            + "let $config := admin:get-configuration()\n"
            + "let $db := xdmp:database(\"" + dbName + "\")\n"
            + "for $f in xdmp:database-forests($db)\n "
//            + "where admin:database-is-forest-retired($config, $db, $f) eq fn:false()\n"
            + "return xdmp:forest-clear($f)";
        runQuery(xccUri, q);
        session.close();
        Thread.sleep(3000);
    }
    
    public static void setBucketPolicy(String xccUri)
        throws XccConfigException, RequestException, URISyntaxException {
        String query = "xquery version \"1.0-ml\"\n;"
            + "import module namespace admin = \"http://marklogic.com/xdmp/admin\" at \"/MarkLogic/admin.xqy\";\n"
            + "let $config := admin:get-configuration()\n"
            + "let $dbid := xdmp:database()\n"
            + "let $bucket-policy := admin:bucket-assignment-policy()\n"
            + "let $config := admin:database-set-assignment-policy($config, $dbid, $bucket-policy)\n"
            + "let $config := admin:database-set-rebalancer-enable($config, $dbid, fn:false())\n"
            + "return admin:save-configuration($config)";
        runQuery(xccUri, query);
    }
    
    
    public static void partitionDelete(String xccUri, String partitionName)
        throws XccConfigException, RequestException, URISyntaxException {
        String query = "xquery version \"1.0-ml\"\n;"
        + "import module namespace ts = \"http://marklogic.com/xdmp/tieredstorage\"\n"
        +      "at \"/MarkLogic/tieredstorage.xqy\";\n"
        + "ts:partition-delete(xdmp:database(\"Documents\"),\""+ partitionName + "\",xs:boolean(\"true\"))";
        runQuery(xccUri, query);
    }
    
    /**
     * get uris of all non-empty documents
     * 
     * @param xccUri
     * @return
     * @throws XccConfigException
     * @throws RequestException
     * @throws URISyntaxException
     */
    public static ResultSequence getNonEmptyDocsURIs(String xccUri) throws XccConfigException,
        RequestException, URISyntaxException {
        String q = "xquery version \"1.0-ml\";" + 
            "let $uris := \n" +
                "for $doc in fn:collection() " +
                "where fn:empty($doc) eq fn:false() \n" + 
                "return <uri>{fn:base-uri($doc)}</uri>\n" +
            "return\n" +
            "for $uri in $uris order by $uri/text() return $uri";
        return runQuery(xccUri, q);
    }
    
    /**
     * Get all uris and document contents from the datasource of the XccUri
     * 
     * @param xccUri
     * @return
     * @throws XccConfigException
     * @throws RequestException
     * @throws URISyntaxException
     */
    public static ResultSequence getAllDocs(String xccUri) throws XccConfigException,
        RequestException, URISyntaxException {
        String q = "xquery version \"1.0-ml\";"
            + "for $doc in fn:collection() "
            + "let $uri := fn:base-uri($doc) "
            + "where fn:empty($doc) eq fn:false() \n"
            + "order by $uri " + "return ($uri, $doc/node()) ";
        return runQuery(xccUri, q);
    }
    
    /**
     * Get all document contents from the datasource of the XccUri
     * 
     * @param xccUri
     * @return
     * @throws XccConfigException
     * @throws RequestException
     * @throws URISyntaxException
     */
    public static ResultSequence getOnlyDocs(String xccUri) throws XccConfigException,
        RequestException, URISyntaxException {
        String q = "xquery version \"1.0-ml\";"
            + "for $doc in fn:collection() "
            + "let $uri := fn:base-uri($doc) "
            + "where fn:empty($doc) eq fn:false() \n"
            + "order by $uri " + "return ($doc/node()) ";
        return runQuery(xccUri, q);
    }
    
    public static String readSmallFile(String filename) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(filename));
        StringBuilder content = new StringBuilder();
        String line;
        while( (line = br.readLine()) != null) {
            content.append(line + newLine);
        }
        br.close();
        return content.toString().trim();
    }

    public static void writeFile(String filename, StringBuilder sb)
        throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(
            filename)));
        bw.write(sb.toString());
        bw.close();
    }
    
    public static void writeFile(String filename, String str)
        throws IOException {
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(
            filename)));
        bw.write(str);
        bw.close();
    }
    
    public static void deleteDirectory(File f) throws IOException {
        if (!f.exists()) {
            return;
        }
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                deleteDirectory(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

    public static void closeSession() {
        if (session!= null && !session.isClosed()) {
            session.close();
        }
    }
}
