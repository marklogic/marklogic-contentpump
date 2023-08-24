/*
 * Copyright (c) 2023 MarkLogic Corporation
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
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.lang.RiotParsers;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.system.*;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.sparql.core.Quad;
import com.marklogic.contentpump.utilities.FileIterator;
import com.marklogic.contentpump.utilities.IdGenerator;
import com.marklogic.contentpump.utilities.PermissionUtil;
import com.marklogic.mapreduce.LinkedMapWritable;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;

/**
 * Reader for RDF quads/triples. Uses Jena library to parse RDF and sends triples
 * to the database in groups of MAXTRIPLESPERDOCUMENT.
 * 
 * @author nwalsh
 *
 * @param <VALUEIN>
 */
public class RDFReader<VALUEIN> extends ImportRecordReader<VALUEIN> {
    public static final Log LOG = LogFactory.getLog(RDFReader.class);
    public static final String HASHALGORITHM = "SHA-256";
    public static final String DEFAULT_GRAPH = "http://marklogic.com/semantics#default-graph";
    public static final String JENA_DEFAULT_GRAPH = "urn:x-arq:DefaultGraphNode";
    protected static Pattern[] patterns = new Pattern[] {
            Pattern.compile("&"), Pattern.compile("<"), Pattern.compile(">") };

    protected int MAXTRIPLESPERDOCUMENT = 100;
    protected int MAXGRAPHSPERREQUEST = 100;
    protected int countPerBatch = 0;
    protected long INMEMORYTHRESHOLD = 1 * 1024 * 1000; // 1Mb
    protected long INGESTIONNOTIFYSTEP = 10000;

    protected Dataset dataset = null;
    protected StmtIterator statementIter = null;
    protected Iterator<String> graphNameIter = null;
    protected String collection = null;
    protected RunnableParser jenaStreamingParser = null;
    protected Iterator rdfIter;
    protected Stream rdfInputStream;
    protected Lang lang;

    protected Hashtable<String, Vector> collectionHash =
        new Hashtable<>();
    protected int collectionCount = 0;
    private static final int MAX_COLLECTIONS = 100;
    protected boolean ignoreCollectionQuad = false;
    protected boolean hasOutputCol = false;
    protected String outputGraph;
    protected String outputOverrideGraph;
    protected StringBuilder buffer;
    protected boolean hasNext = true;
    protected IdGenerator idGen;

    protected Random random;
    protected long randomValue;
    protected long milliSecs;
    private long HASH64_STEP = 15485863L;

    protected String origFn;
    // Tracks input filename even in the CompressedRDFReader case
    protected String inputFn; 
    protected long splitStart;
    protected long start;
    /*stays 0 until we're done*/
    protected long pos;
    protected long end;
    protected boolean compressed;
    protected long ingestedTriples = 0;
    /* new graphs identified within a RDFReader */
    protected HashSet<String> newGraphs;
    protected HashMap<String,ContentPermission[]> existingMapPerms;
    protected Iterator<String> graphItr;
    /* server version */
    protected String version;
    protected LinkedMapWritable roleMap;
    protected ContentPermission[] defaultPerms;
    protected StringBuilder graphQry;
    /* hadoop:get-role-map() only exists in ML 8.0-1~8.0-3 */
    protected boolean roleMapExists;
    protected boolean graphSupported;
    
    private static final Object jenaLock = new Object();
    
    public RDFReader(String version, LinkedMapWritable roleMap) {
        random = new Random();
        randomValue = random.nextLong();
        Calendar cal = Calendar.getInstance();
        milliSecs = cal.getTimeInMillis();
        compressed = false;
        this.version = version;
        this.roleMap = roleMap;
        roleMapExists = roleMap!=null && roleMap.size()>0 ;
        graphQry = new StringBuilder();
        existingMapPerms = new HashMap<>();
        newGraphs = new HashSet<>();
    }

    @Override
    public void close() throws IOException {
        //report total counts of triples on close
        LOG.info("Ingested " + ingestedTriples + " triples from " + origFn);
        if(rdfInputStream!=null) {
            rdfInputStream.close();
        }
        dataset = null;
        if(graphQry.length()==0) 
            return;
        //create graph doc in a batch
        submitGraphQuery();
    }

    protected void submitGraphQuery() throws IOException{
        Session session = null;
        ContentSource cs;
        try {
            cs = InternalUtilities.getOutputContentSource(conf,
                conf.getStrings(MarkLogicConstants.OUTPUT_HOST)[0]);
            session = cs.newSession();
            RequestOptions options = new RequestOptions();
            options.setDefaultXQueryVersion("1.0-ml");
            session.setDefaultRequestOptions(options);
            AdhocQuery query = session.newAdhocQuery(graphQry.toString());
            if(LOG.isDebugEnabled()) {
                LOG.debug(graphQry.toString());
            }
            query.setOptions(options);
            session.submitRequest(query);
        } catch (RequestException e) {
            throw new IOException(e);
        } catch (XccConfigException e) {
            throw new IOException(e);
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }
    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (!hasNext) {
            return 1;
        }
        return (pos > end) ? 1 : ((float) (pos - start)) / (end - start);
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        if (version == null)
            throw new IOException("Server Version is null");
        String majorVersion = version.substring(0, version.indexOf('.'));
        graphSupported = Integer.valueOf(majorVersion) >= 8;
        conf = context.getConfiguration();

        String rdfopt = conf.get(ConfigConstants.RDF_STREAMING_MEMORY_THRESHOLD);
        if (rdfopt != null) {
            INMEMORYTHRESHOLD = Long.parseLong(rdfopt);
        }

        rdfopt = conf.get(ConfigConstants.RDF_TRIPLES_PER_DOCUMENT);
        if (rdfopt != null) {
            MAXTRIPLESPERDOCUMENT = Integer.parseInt(rdfopt);
        }

        String fnAsColl = conf.get(ConfigConstants.CONF_OUTPUT_FILENAME_AS_COLLECTION);
        if (fnAsColl != null) {
            LOG.warn("The -filename_as_collection has no effect with input_type RDF, use -output_collections instead.");
        }

        String[] collections = conf.getStrings(MarkLogicConstants.OUTPUT_COLLECTION);
        outputGraph = conf.get(MarkLogicConstants.OUTPUT_GRAPH);
        outputOverrideGraph = conf.get(MarkLogicConstants.OUTPUT_OVERRIDE_GRAPH);
        //if no defulat-graph set and output_collections is set
        ignoreCollectionQuad = (outputGraph == null && collections != null)
            || outputOverrideGraph != null;
        hasOutputCol = (collections != null);

        Class<? extends Writable> valueClass = RDFWritable.class;

        @SuppressWarnings("unchecked")
        VALUEIN localValue = (VALUEIN) ReflectionUtils.newInstance(valueClass, 
                conf);

        value = localValue;
        encoding = conf.get(MarkLogicConstants.OUTPUT_CONTENT_ENCODING,
                DEFAULT_ENCODING);

        setFile(((FileSplit) inSplit).getPath());
        fs = file.getFileSystem(context.getConfiguration());
        
        FileStatus status = fs.getFileStatus(file);
        if(status.isDirectory()) {
            iterator = new FileIterator((FileSplit)inSplit, context);
            inSplit = iterator.next();
        }

        try {
            initStream(inSplit);
        } catch (IOException e ){
            LOG.error("Invalid input: " + file.getName() + ": " + e.getMessage());
            throw e;
        }
        String[] perms = conf.getStrings(MarkLogicConstants.OUTPUT_PERMISSION);
        if(perms!=null) {
            defaultPerms = PermissionUtil.getPermissions(perms).toArray(
                new ContentPermission[perms.length>>1]);
        } else {
            List<ContentPermission> tmp = PermissionUtil.getDefaultPermissions(conf,roleMap);
            if(tmp!=null)
                defaultPerms = tmp.toArray(new ContentPermission[tmp.size()]);
        }
            
        if (roleMapExists) 
            initExistingMapPerms();
    }

    protected void initStream(InputSplit inSplit)
            throws IOException, InterruptedException {
        FSDataInputStream in = openFile(inSplit, false);
        if (in == null) {
            return;
        }
        long size = inSplit.getLength();
        initParser(file.toUri().toASCIIString(), size);
        parse(file.getName(), in);
    }

    protected void initParser(String fsname, long size) throws IOException {
        start = 0;
        pos = 0;
        end = 1;
        jenaStreamingParser = null;
        dataset = null;
        statementIter = null;
        graphNameIter = null;

        String ext = null;
        if (fsname.contains(".")) {
            int pos = fsname.lastIndexOf(".");
            ext = fsname.substring(pos);
            if (".gz".equals(ext)) {
                fsname = fsname.substring(0, pos);
                pos = fsname.lastIndexOf(".");
                if (pos >= 0) {
                    ext = fsname.substring(pos);
                } else {
                    ext = null;
                }
            }
        }
        origFn = fsname;
        inputFn = Long.toHexString(fuse(scramble(random.nextLong()),
                fuse(scramble(milliSecs), random.nextLong())));
        idGen = new IdGenerator(inputFn + "-" + splitStart);
        lang = null;
        if (".rdf".equals(ext)) {
            lang = Lang.RDFXML;
        } else if (".ttl".equals(ext)) {
            lang = Lang.TURTLE;
        } else if (".json".equals(ext)) {
            lang = Lang.RDFJSON;
        } else if (".n3".equals(ext)) {
            lang = Lang.N3;
        } else if (".nt".equals(ext)) {
            lang = Lang.NTRIPLES;
        } else if (".nq".equals(ext)) {
            lang = Lang.NQUADS;
        } else if (".trig".equals(ext)) {
            lang = Lang.TRIG;
        } else {
            lang = Lang.RDFXML; // We have to default to something!
        }
        synchronized (jenaLock) {
            if (size < INMEMORYTHRESHOLD) {
                dataset = DatasetFactory.createTxnMem();
            }
        }
    }

    protected void parse(String fsname, FSDataInputStream in)
            throws IOException {
        try {
            loadModel(fsname, in);
        } catch (Exception e) {
            LOG.error("Failed to parse(please check intactness and encoding): " + origFn);
        }
    }

    protected void loadModel(final String fsname, final InputStream in) throws IOException {
        if (dataset == null) {
            // Previously during parsing, we create a runnable thread for each file and run them everytime.
            // It has been replaced by direct calls to the Jena parser because
            // Asyncparser manages its parsing in seperate thread. 
            jenaStreamingParser = new RunnableParser(origFn, fsname, in, lang);
            jenaStreamingParser.run();
        } else {
            StreamRDF dest = StreamRDFLib.dataset(dataset.asDatasetGraph());
            ErrorHandler handler = new ParserErrorHandler(fsname);
            try {
                AsyncParser.of(RDFParserBuilder.create().source(in).lang(lang).errorHandler(handler).base(fsname)).asyncParseSources(dest);

            } catch (Throwable e) {
                LOG.error("Parse error in RDF document(please check intactness and encoding); skipping this document:"
                    + fsname + " " + e.getMessage());
            }
            in.close();
            graphNameIter = dataset.listNames();
            statementIter = dataset.getDefaultModel().listStatements();
        }

        // We don't know how many statements are in the model; we could count them, but that's
        // possibly expensive. So we just say 0 until we're done.
        pos = 0;
        end = 1;
    }

    protected void write(String str) {
        if (buffer == null) {
            buffer = new StringBuilder();
        }
        buffer.append(str);
    }


    private long rotl(long x, long y)
    {
        return (x<<y)^(x>>(64-y));
    }

    private long fuse(long a, long b)
    {
        return rotl(a,8)^b;
    }

    private long scramble(long x)
    {
        return x^rotl(x,20)^rotl(x,40);
    }

    private long hash64(long value, String str) {
        char[] arr = str.toCharArray();
        for (int i = 0; i < str.length(); i++) {
            value = (value + Character.getNumericValue(arr[i])) * HASH64_STEP;
        }
        return value;
    }

    protected String resource(Node rsrc) {
        if (rsrc.isBlank()) {
            return "http://marklogic.com/semantics/blank/" + Long.toHexString(
                        hash64(fuse(scramble(milliSecs),randomValue), rsrc.getBlankNodeLabel()));
        } else {
            return escapeXml(rsrc.toString());
        }
    }

    protected String resource(Node rsrc, String tag) {
        String uri = resource(rsrc);
        return "<sem:" + tag + ">" + uri + "</sem:" + tag + ">";
    }

    private String resource(Resource rsrc) {
        if (rsrc.isAnon()) {
            return "http://marklogic.com/semantics/blank/" + Long.toHexString(
                    hash64(fuse(scramble(milliSecs),randomValue), rsrc.getId().getLabelString()));
        } else {
            return escapeXml(rsrc.toString());
        }
    }

    protected String resource(Resource rsrc, String tag) {
        String uri = resource(rsrc);
        return "<sem:" + tag + ">" + uri + "</sem:" + tag + ">";
    }

    protected String subject(Node subj) {
        return resource(subj, "subject");
    }

    protected String subject(Resource subj) {
        return resource(subj, "subject");
    }

    protected String predicate(Node subj) {
        return resource(subj, "predicate");
    }

    protected String predicate(Resource subj) {
        return resource(subj, "predicate");
    }

    protected String object(Node node) {
        if (node.isLiteral()) {
            String text = node.getLiteralLexicalForm();
            String type = node.getLiteralDatatypeURI();
            String lang = node.getLiteralLanguage();

            if (lang == null || "".equals(lang)) {
                lang = "";
            } else {
                lang = " xml:lang='" + escapeXml(lang) + "'";
            }

            if ("".equals(lang)) {
                if (type == null) {
                    type = "http://www.w3.org/2001/XMLSchema#string";
                }
                type = " datatype='" + escapeXml(type) + "'";
            } else {
                type = "";
            }

            return "<sem:object" + type + lang + ">" + escapeXml(text) + "</sem:object>";
        } else if (node.isBlank()) {
            return "<sem:object>http://marklogic.com/semantics/blank/" + Long.toHexString(
                    hash64(fuse(scramble(milliSecs),randomValue), node.getBlankNodeLabel()))
                    +"</sem:object>";
        } else {
            return "<sem:object>" + escapeXml(node.toString()) + "</sem:object>";
        }
    }

    private String object(RDFNode node) {
        if (node.isLiteral()) {
            Literal lit = node.asLiteral();
            String text = lit.getString();
            String lang = lit.getLanguage();
            String type = lit.getDatatypeURI();

            if (lang == null || "".equals(lang)) {
                lang = "";
            } else {
                lang = " xml:lang='" + escapeXml(lang) + "'";
            }

            if ("".equals(lang)) {
                if (type == null) {
                    type = "http://www.w3.org/2001/XMLSchema#string";
                }
                type = " datatype='" + escapeXml(type) + "'";
            } else {
                type = "";
            }

            return "<sem:object" + type + lang + ">" + escapeXml(text) + "</sem:object>";
        } else if (node.isAnon()) {
            return "<sem:object>http://marklogic.com/semantics/blank/" + Long.toHexString(
                    hash64(fuse(scramble(milliSecs),randomValue), node.toString()))
                    +"</sem:object>";
        } else {
            return "<sem:object>" + escapeXml(node.toString()) + "</sem:object>";
        }
    }

    protected static String escapeXml(String _in) {
        if (null == _in){
            return "";
        }
        return patterns[2].matcher(
                    patterns[1].matcher(
                        patterns[0].matcher(_in).replaceAll("&amp;"))
                            .replaceAll("&lt;")).replaceAll("&gt;");
    }

    protected void setKey() {
        setKey(idGen.incrementAndGet() +".xml", 0, 0, true);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean result = false;
        if (jenaStreamingParser == null || !jenaStreamingParser.failed()) {
            if (statementIter == null) {
                result = nextStreamingKeyValue();
            } else {
                result = nextInMemoryKeyValue();
            }
        }

        return result;
    }
    
    public void initExistingMapPerms() throws IOException {
        Session session = null;
        ResultSequence result = null;
        ContentSource cs;
        try {
            cs = InternalUtilities.getOutputContentSource(conf,
                conf.getStrings(MarkLogicConstants.OUTPUT_HOST)[0]);
            session = cs.newSession();
            RequestOptions options = new RequestOptions();
            options.setDefaultXQueryVersion("1.0-ml");
            session.setDefaultRequestOptions(options);
            StringBuilder sb = new StringBuilder();
            sb.append("xquery version \"1.0-ml\";\n");
            sb.append("for $doc in fn:collection(\"http://marklogic.com/semantics#graphs\")");
            sb.append("return (fn:base-uri($doc),for $p in $doc/*:graph/*:permissions/*:permission return ($p/*:role-id/text(),$p/*:capability/text()),\"0\")");
            
            if(LOG.isDebugEnabled()) {
                LOG.debug(sb.toString());
            }
            AdhocQuery query = session.newAdhocQuery(sb.toString());
            query.setOptions(options);
            result = session.submitRequest(query);
            while (result.hasNext()) {
                String uri = result.next().asString();
                String tmp = result.next().asString();
                ArrayList<ContentPermission> perms = new ArrayList<>();
                while(!tmp.equals("0")) {
                    Text roleid = new Text(tmp);
                    if (!result.hasNext()) {
                        throw new IOException("Invalid role map");
                    }
                    String roleName = roleMap.get(roleid).toString();
                    String cap = result.next().asString();
                    ContentCapability capability = PermissionUtil
                        .getCapbility(cap);
                    perms.add(new ContentPermission(capability, roleName));
                    tmp = result.next().asString();
                }
                
                existingMapPerms.put(uri, perms.toArray(new ContentPermission[perms.size()]));
                
            }
        } catch (XccConfigException e) {
            throw new IOException(e);
        } catch (RequestException e) {
            throw new IOException(e);
        } finally {
            if (result != null) {
                result.close();
            }
            if (session != null) {
                session.close();
            }
        }
    }
    
    /* 
     * create graph doc
     * 
     * return ContentPermission[] for the graph
     */
    public ContentPermission[] insertGraphDoc(String graph) throws IOException {
        ArrayList<ContentPermission> perms = new ArrayList<>();
            ContentPermission[] permissions = defaultPerms;
            StringBuilder sb = graphQry;
            if (countPerBatch >= MAXGRAPHSPERREQUEST) {
                countPerBatch = 0;
                submitGraphQuery();
                graphQry.setLength(0);
            }
            String escapedGraph = escapeXml(graph);
            sb.append("if(fn:empty(fn:doc(\"").append(escapedGraph)
                .append("\"))) then sem:create-graph-document(sem:iri(\"")
                .append(escapedGraph).append("\"),(");
            if (permissions != null && permissions.length > 0) {
                for (int i = 0; i < permissions.length; i++) {
                    ContentPermission cp = permissions[i];
                    if (i > 0)
                        sb.append(",");
                    sb.append("xdmp:permission(\"");
                    sb.append(cp.getRole());
                    sb.append("\",\"");
                    sb.append(cp.getCapability());
                    sb.append("\")");
                }
                sb.append(")");
            } else {
                sb.append("xdmp:default-permissions())");
            }
            sb.append(") else ();\n");
            countPerBatch++;
        return perms.toArray(new ContentPermission[0]);
    }

    public boolean nextInMemoryKeyValue() throws IOException, InterruptedException {
        if (lang == Lang.NQUADS || lang == Lang.TRIG) {
            return nextInMemoryQuadKeyValue();
        } else {
            return nextInMemoryTripleKeyValue();
        }
    }

    public boolean nextInMemoryTripleKeyValue() throws IOException, InterruptedException {
        if(statementIter == null) return false;
        try {
            if (!statementIter.hasNext()) {
                hasNext = false;
                return false;
            }
        } catch (Exception ex){
            LOG.error("Parse error in RDF document(please check intactness and encoding); skipping this document: "
                    + origFn + " " + ex.getMessage());
            return false;
        }
        setKey();
        write("<sem:triples xmlns:sem='http://marklogic.com/semantics'>\n");
        write("<sem:origin>" + origFn + "</sem:origin>\n");
        int max = MAXTRIPLESPERDOCUMENT;
        while (max > 0 && statementIter.hasNext()) {
            Statement stmt = statementIter.nextStatement();
            write("<sem:triple>");
            write(subject(stmt.getSubject()));
            write(predicate(stmt.getPredicate()));
            write(object(stmt.getObject()));
            write("</sem:triple>\n");
            notifyUser();
            max--;
        }
        write("</sem:triples>\n");

        if (!statementIter.hasNext()) {
            pos = 1;
        }

        writeValue();
        return true;
    }

    public boolean nextInMemoryQuadKeyValue() throws IOException, InterruptedException {
        if (ignoreCollectionQuad) {
            return nextInMemoryQuadKeyValueIgnoreCollections();
        } else {
            return nextInMemoryQuadKeyValueWithCollections();
        }
    }

    public boolean nextInMemoryQuadKeyValueWithCollections() throws IOException, InterruptedException {
        if(statementIter == null) return false;
        try {
            while (!statementIter.hasNext()) {
                if (graphNameIter.hasNext()) {
                    collection = graphNameIter.next();
                    statementIter = dataset.getNamedModel(collection).listStatements();
                } else {
                    hasNext = false;
                    collection = null;
                    return false;
                }
            }
        } catch (Exception ex){
            LOG.error("Parse error in RDF document(please check intactness and encoding); skipping this document: "
                    + origFn + " " + ex.getMessage());
            return false;
        }
        setKey();
        write("<sem:triples xmlns:sem='http://marklogic.com/semantics'>");
        int max = MAXTRIPLESPERDOCUMENT;
        while (max > 0 && statementIter.hasNext()) {
            Statement stmt = statementIter.nextStatement();
            write("<sem:triple>");
            write(subject(stmt.getSubject()));
            write(predicate(stmt.getPredicate()));
            write(object(stmt.getObject()));
            write("</sem:triple>");
            max--;
            notifyUser();
        }
        write("</sem:triples>\n");

        if (!statementIter.hasNext()) {
            pos = 1;
        }

        writeValue(collection);
        return true;
    }

    public boolean nextInMemoryQuadKeyValueIgnoreCollections() throws IOException, InterruptedException {
        if(statementIter == null) return false;
        try {
            while (!statementIter.hasNext()) {
                if (graphNameIter.hasNext()) {
                    collection = graphNameIter.next();
                    statementIter = dataset.getNamedModel(collection).listStatements();
                } else {
                    hasNext = false;
                    return false;
                }
            }
        } catch (Exception ex){
            LOG.error("Parse error in RDF document(please check intactness and encoding); skipping this document: "
                    + origFn + " " + ex.getMessage());
            return false;
        }
        setKey();
        write("<sem:triples xmlns:sem='http://marklogic.com/semantics'>");
        int max = MAXTRIPLESPERDOCUMENT;
        while (max > 0 && statementIter.hasNext()) {
            Statement stmt = statementIter.nextStatement();
            write("<sem:triple>");
            write(subject(stmt.getSubject()));
            write(predicate(stmt.getPredicate()));
            write(object(stmt.getObject()));
            write("</sem:triple>");
            notifyUser();
            max--;

            boolean moreTriples = statementIter.hasNext();
            while (!moreTriples) {
                moreTriples = true; // counter-intuitive; get out of the loop if we really are finished
                if (graphNameIter.hasNext()) {
                    collection = graphNameIter.next();
                    statementIter = dataset.getNamedModel(collection).listStatements();
                    moreTriples = statementIter.hasNext();
                }
            }
        }
        write("</sem:triples>\n");

        if (!statementIter.hasNext()) {
            pos = 1;
        }

        writeValue();
        return true;
    }

    public boolean nextStreamingKeyValue() throws IOException, InterruptedException {
        if(rdfIter == null) return false;
        if (!rdfIter.hasNext() && collectionHash.size() == 0) {
            if(compressed) {
                hasNext = false;
                return false;
            } else {
                if (iterator!=null && iterator.hasNext()) {
                    close();
                    initStream(iterator.next());
                } else {
                    hasNext = false;
                    return false;
                }
            }
        }
        if (lang == Lang.NQUADS || lang == Lang.TRIG) {
            return nextStramingQuadKeyValue();
        } else {
            return nextStreamingTripleKeyValue();
        }
    }

    protected boolean nextStreamingTripleKeyValue() throws IOException, InterruptedException {
        if(rdfIter == null) return false;
        setKey();
        write("<sem:triples xmlns:sem='http://marklogic.com/semantics'>");
        int max = MAXTRIPLESPERDOCUMENT;
        try {
            while (max > 0 && rdfIter.hasNext()) {
                Triple triple = (Triple) rdfIter.next();
                write("<sem:triple>");
                write(subject(triple.getSubject()));
                write(predicate(triple.getPredicate()));
                write(object(triple.getObject()));
                write("</sem:triple>");
                notifyUser();
                max--;
            }
                write("</sem:triples>\n");
        } catch (Exception ex){
            LOG.error("Parse error in RDF document(please check intactness and encoding); skipping this document: "
                    + origFn + " " + ex.getMessage());
            return false;
        }
        if (!rdfIter.hasNext()) {
            pos = 1;
        }

        writeValue();
        return true;
    }

    public boolean nextStramingQuadKeyValue() throws IOException, InterruptedException {
        if (ignoreCollectionQuad) {
            return nextStreamingQuadKeyValueIgnoreCollections();
        } else {
            return nextStreamingQuadKeyValueWithCollections();
        }
    }

    protected boolean nextStreamingQuadKeyValueIgnoreCollections() throws IOException, InterruptedException {
        if(rdfIter == null) return false;
        setKey();
        write("<sem:triples xmlns:sem='http://marklogic.com/semantics'>");
        int max = MAXTRIPLESPERDOCUMENT;
        try {
            while (max > 0 && rdfIter.hasNext()) {
                Quad quad = (Quad) rdfIter.next();
                write("<sem:triple>");
                write(subject(quad.getSubject()));
                write(predicate(quad.getPredicate()));
                write(object(quad.getObject()));
                write("</sem:triple>");
                notifyUser();
                max--;
            }
            write("</sem:triples>\n");
        } catch (Exception ex){
            LOG.error("Parse error in RDF document(please check intactness and encoding); skipping this document: "
                    + origFn + " " + ex.getMessage());
            return false;
        }
        if (!rdfIter.hasNext()) {
            pos = 1;
        }

        writeValue();
        return true;
    }

    public boolean nextStreamingQuadKeyValueWithCollections() throws IOException, InterruptedException {
        if(rdfIter == null) return false;
        if (!rdfIter.hasNext() && collectionHash.isEmpty()) {
            hasNext = false;
            return false;
        }

        String collection = null;
        boolean overflow = false;
        try {
            while (!overflow && rdfIter.hasNext()) {
                Quad quad= (Quad) rdfIter.next();
                Node graph = quad.getGraph();
                if (graph == null) {
                    collection = DEFAULT_GRAPH;
                } else {
                    collection = resource(quad.getGraph());
                }

                String triple = subject(quad.getSubject())
                        + predicate(quad.getPredicate())
                        + object(quad.getObject());

                if (!collectionHash.containsKey(collection)) {
                    collectionHash.put(collection, new Vector<String>());
                    collectionCount++;
                    //System.err.println("Added " + collection + " (" + collectionHash.keySet().size() + ")");
                } else {
                    //System.err.println("      " + collection + " (" + collectionHash.get(collection).size() + ")");
                }

                @SuppressWarnings("unchecked")
                Vector<String> triples = collectionHash.get(collection);

                triples.add("<sem:triple>" + triple + "</sem:triple>");

                //System.err.println(triple);

                if (triples.size() == MAXTRIPLESPERDOCUMENT) {
                    //System.err.println("Full doc " + collection + " (" + triples.size() + ")");
                    overflow = true;
                } else if (collectionCount > MAX_COLLECTIONS) {
                    collection = largestCollection();
                    //System.err.println("Full hsh " + collection + " (" + collectionHash.get(collection).size() + ")");
                    overflow = true;
                }
            }
        } catch (Exception ex){
            LOG.error("Parse error in RDF document(please check intactness and encoding); skipping this document:"
                    + origFn + " " + ex.getMessage());
            return false;
        }

        if (!overflow) {
            // HACK: fix this!
            for (String c : collectionHash.keySet()) {
                collection = c;
                //System.err.println("Flushing " + collection + " (" + collectionHash.get(collection).size() + ")");
                break;
            }
        }
        Vector<String> triples = collectionHash.get(collection);
        setKey();
        write("<sem:triples xmlns:sem='http://marklogic.com/semantics'>");
        for (String t : triples) {
            write(t);
            notifyUser();
        }
        write("</sem:triples>\n");

        collectionHash.remove(collection);
        collectionCount--;

        if (!rdfIter.hasNext()) {
            pos = 1;
        }

        writeValue(collection);
        return true;
    }

    public void writeValue() throws IOException {
        writeValue(null);
    }

    public void writeValue(String collection) throws IOException {
        if (value instanceof Text) {
            ((Text) value).set(buffer.toString());
        } else if (value instanceof RDFWritable) {
            ((RDFWritable)value).set(buffer.toString());
            
            if (collection != null) {
                if(collection.equals(JENA_DEFAULT_GRAPH)) {
                    collection = null;
                }
            }
            
            if(hasOutputCol){// output_collections is set
               if(outputOverrideGraph!=null) {
                    collection = outputOverrideGraph;
                } else if(outputGraph != null) {
                    if (collection == null) {
                        //no graph specified in quad, use output_graph
                        collection = outputGraph;
                    }
                } else { // no output_graph or output_override_graph
                    String[] outCols = conf
                    .getStrings(MarkLogicConstants.OUTPUT_COLLECTION);
                collection = outCols[0];
                }
            } else {//no output_collections
                if (collection == null) { //no quad in data
                    collection = outputGraph != null ? outputGraph : outputOverrideGraph;
                    if(collection == null)
                        collection = DEFAULT_GRAPH;
                }
            }
            if (roleMapExists && existingMapPerms.containsKey(collection)) {
                ((RDFWritable)value).setPermissions(existingMapPerms.get(collection));
            } else {
                ((RDFWritable)value).setPermissions(defaultPerms);
            }
            if (graphSupported && !newGraphs.contains(collection)) {
                newGraphs.add(collection);
                insertGraphDoc(collection);
            }
            ((RDFWritable)value).setGraph(collection);
        } else {
            ((Text)((ContentWithFileNameWritable<VALUEIN>)
                    value).getValue()).set(buffer.toString());
        }
        buffer.setLength(0);
    }

    protected String largestCollection() {
        String collection = "";
        int count = -1;

        for (String c : collectionHash.keySet()) {
            if (collectionHash.get(c).size() > count) {
                count = collectionHash.get(c).size();
                collection = c;
            }
        }

        return collection;
    }

    protected void notifyUser() {
        ingestedTriples++;
        if (ingestedTriples % INGESTIONNOTIFYSTEP == 0) {
            LOG.info("Ingested " + ingestedTriples + " triples from " + origFn);
        }
    }

    protected class ParserErrorHandler implements ErrorHandler {
        String inputfn = "";

        public ParserErrorHandler(String inputfn) {
            this.inputfn = inputfn;
        }

        private String formatMessage(String message, long line, long col) {
            String msg = inputfn + ":";
            if (line >= 0) {
                msg += line;
            }
            if (line >= 0 && col >= 0) {
                msg += ":" + col;
            }
            return msg += " " + message;
        }

        @Override
        public void warning(String message, long line, long col) {
            //For Bug 24519, to improve readability of logs, I have chose to log the warnings for IRI only in debug mode.
            if (message.contains("Bad IRI:") || message.contains("Illegal character in IRI") || message.contains("Not advised IRI")) {
                LOG.debug(formatMessage(message, line, col));
            } else {
                LOG.warn(formatMessage(message, line, col));
            }
        }

        @Override
        public void error(String message, long line, long col) {
            //For Bug 24519, to improve readability of logs, I have chose to log the error for IRI only in debug mode.
            if (message.contains("Bad character in IRI") || message.contains("Problem setting StAX property")) {
                LOG.debug(formatMessage(message, line, col));
            } else {
                LOG.error(formatMessage(message, line, col));
            }
        }

        @Override
        public void fatal(String message, long line, long col) {
            LOG.fatal(formatMessage(message, line, col));
        }
    }

    protected class RunnableParser {
        final String fsname;
        final InputStream in;
        final String origFn;
        final Lang lang;
        private boolean failed = false;
        
        public RunnableParser(String origFn, String fsname, InputStream in, Lang lang) {
            super();
            this.fsname = fsname;
            this.in = in;
            this.origFn = origFn;
            this.lang = lang;

            if(LOG.isDebugEnabled())
                LOG.debug("O:" + origFn + " : " + fsname);
        }

        public boolean failed() {
            return failed;
        }


        public void run() {
            // Our streamed RDF code flow is based on the parser output written and iterated via a stream. 
            // So I chose to use the stream-based API calls.

            // For Ntriples and Nquads, the different API calls were suggested by the Jena documentation. 
            // For the Trig and else part, they are the same API calls but the difference is what they emit after parsing. 
            // Trig file outputs are emitted as quads and everything else like ttl, json, RDFXML file outputs are emitted as triples. 
            // Hence same API calls with different stream calls.
            try {
                ErrorHandler handler = new ParserErrorHandler(fsname);
                ParserProfile prof = RiotLib.profile(lang, fsname, handler);
                if (lang == Lang.TRIG) {
                    rdfInputStream = AsyncParser.of(RDFParserBuilder.create().source(in).lang(lang).errorHandler(handler).base(fsname)).streamQuads();
                    rdfIter = rdfInputStream.iterator();
                } else if (lang == Lang.NTRIPLES) {
                    rdfIter = RiotParsers.createIteratorNTriples(in, prof);
                } else if (lang == Lang.NQUADS) {
                    rdfIter = RiotParsers.createIteratorNQuads(in, prof);
                }else {
                    rdfInputStream = AsyncParser.of(RDFParserBuilder.create().source(in).lang(lang).errorHandler(handler).base(fsname)).streamTriples();
                    rdfIter = rdfInputStream.iterator();
                }
            } catch (Exception ex) {
                failed = true;
                LOG.error("Parse error in RDF document(please check intactness and encoding); skipping this document: "
                    + origFn + " " + ex.getMessage());
            }
        }
    }
}
