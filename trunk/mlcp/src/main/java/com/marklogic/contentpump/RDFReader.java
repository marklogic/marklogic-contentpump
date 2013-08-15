/*
  Copyright 2003-2013 MarkLogic Corporation
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
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.PipedQuadsStream;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;
import com.marklogic.contentpump.utilities.FileIterator;
import com.marklogic.contentpump.utilities.IdGenerator;
import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.MarkLogicConstants;

import javax.xml.bind.annotation.adapters.HexBinaryAdapter;

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
    protected static Pattern[] patterns = new Pattern[] {
            Pattern.compile("&"), Pattern.compile("<"), Pattern.compile(">") };

    protected int MAXTRIPLESPERDOCUMENT = 100;
    protected long INMEMORYTHRESHOLD = 64 * 1024 * 1000; // 64Mb

    protected Dataset dataset = null;
    protected Model model = null;
    protected StmtIterator statementIter = null;
    protected Iterator<String> graphNameIter = null;
    protected String collection = null;

    protected PipedRDFIterator rdfIter;
    protected PipedRDFStream rdfInputStream;
    protected ExecutorService executor;
    protected Lang lang;

    protected Hashtable<String, Vector> collectionHash = new Hashtable<String, Vector> ();
    protected int collectionCount = 0;
    private static final int MAX_COLLECTIONS = 100;
    protected boolean ignoreCollectionQuad = false;

    protected StringBuilder buffer;
    protected boolean hasNext = true;
    protected IdGenerator idGen;

    protected long randomValue;
    protected long milliSecs;

    protected String inputFn; // Tracks input filename even in the CompressedRDFReader case
    protected long splitStart;
    protected long start;
    protected long pos;
    protected long end;
    protected boolean compressed;
    
    public RDFReader() {
        Random random = new Random();
        randomValue = random.nextLong();
        Calendar cal = Calendar.getInstance();
        milliSecs = cal.getTimeInMillis();
        compressed = false;
    }

    @Override
    public void close() throws IOException {
        if(rdfIter!=null) {
            rdfIter.close();
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
        ignoreCollectionQuad = (collections != null);

        String type = conf.get(MarkLogicConstants.CONTENT_TYPE,
                MarkLogicConstants.DEFAULT_CONTENT_TYPE);
        if (!conf.getBoolean(MarkLogicConstants.OUTPUT_STREAMING, false)) {
            ContentType contentType = ContentType.valueOf(type);
            Class<? extends Writable> valueClass = RDFWritable.class;
            value = (VALUEIN) ReflectionUtils.newInstance(valueClass, conf);
        }
        encoding = conf.get(MarkLogicConstants.OUTPUT_CONTENT_ENCODING);

        // ===================

        file = ((FileSplit) inSplit).getPath();
        fs = file.getFileSystem(context.getConfiguration());
        
        FileStatus status = fs.getFileStatus(file);
        if(status.isDir()) {
            iterator = new FileIterator((FileSplit)inSplit, context);
            inSplit = iterator.next();
        }

        initStream(inSplit);
    }

    protected void initStream(InputSplit inSplit) throws IOException, InterruptedException {
        file = ((FileSplit) inSplit).getPath();
        long size = inSplit.getLength();
        initParser(file.toUri().toASCIIString(), size);
        parse(file.getName());
    }

    protected void initParser(String fsname, long size) throws IOException {
        start = 0;
        pos = 0;
        end = 1;

        dataset = null;
        model = null;
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

        try {
            MessageDigest digest = MessageDigest.getInstance(HASHALGORITHM);
            LOG.info("Hashing: " + fsname);
            inputFn = "/triplestore/" + (new HexBinaryAdapter()).marshal(digest.digest(fsname.getBytes()));
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Could not instantiate hash function for " + HASHALGORITHM);
        }

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

        if (size < INMEMORYTHRESHOLD) {
            if (lang == Lang.NQUADS) {
                dataset = DatasetFactory.createMem();
                model = dataset.getDefaultModel();
            } else {
                model = ModelFactory.createDefaultModel();
            }
            LOG.info("In-memory RDF processing (" + size + " < " + INMEMORYTHRESHOLD + ")");
        } else {
            LOG.info("Streamed RDF processing (" + size + " >= " + INMEMORYTHRESHOLD + ")");
        }

        idGen = new IdGenerator(inputFn + "-" + splitStart);
    }

    protected void parse(String fsname) throws IOException {
        loadModel(fsname, fs.open(file));
    }

    protected void loadModel(String fsname, final InputStream in) throws IOException {
        if (model == null) {
            if (lang == Lang.NQUADS) {
                rdfIter = new PipedRDFIterator<Quad>();
                rdfInputStream = new PipedQuadsStream(rdfIter);
            } else {
                rdfIter = new PipedRDFIterator<Triple>();
                rdfInputStream = new PipedTriplesStream(rdfIter);
            }

            // PipedRDFStream and PipedRDFIterator need to be on different threads
            executor = Executors.newSingleThreadExecutor();

            final String baseURI = fsname;

            // Create a runnable for our parser thread
            Runnable parser = new Runnable() {
                @Override
                public void run() {
                    // Call the parsing process.
                    RDFDataMgr.parse(rdfInputStream, in, baseURI, lang, null);
                }
            };

            // Start the parser on another thread
            executor.submit(parser);
        } else {
            if (dataset == null) {
                RDFDataMgr.read(model, in, lang);
            } else {
                RDFDataMgr.read(dataset, in, lang);
                graphNameIter = dataset.listNames();
            }
            in.close();
            statementIter = model.listStatements();
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

    protected String resource(Node rsrc) {
        if (rsrc.isBlank()) {
            return "http://marklogic.com/semantics/blank/" + Long.toHexString(
                        fuse(scramble((long)rsrc.hashCode()),fuse(scramble(milliSecs),randomValue)));
        } else {
            return escapeXml(rsrc.toString());
        }
    }

    protected String resource(Node rsrc, String tag) {
        String uri = resource(rsrc);
        return "<" + tag + ">" + uri + "</" + tag + ">";
    }

    private String resource(Resource rsrc) {
        if (rsrc.isAnon()) {
            return "http://marklogic.com/semantics/blank/" + Long.toHexString(
                    fuse(scramble((long)rsrc.hashCode()),fuse(scramble(milliSecs),randomValue)));
        } else {
            return escapeXml(rsrc.toString());
        }
    }

    protected String resource(Resource rsrc, String tag) {
        String uri = resource(rsrc);
        return "<" + tag + ">" + uri + "</" + tag + ">";
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

            if (type == null) {
                type = "http://www.w3.org/2001/XMLSchema#string";
            }

            if (lang == null || "".equals(lang)) {
                lang = "";
            } else {
                lang = " xml:lang='" + lang + "'";
            }

            return "<object datatype='" + escapeXml(type) + "'" + lang + ">" + escapeXml(text) + "</object>";
        } else if (node.isBlank()) {
            return "<object>http://marklogic.com/semantics/blank/" + Long.toHexString(
                    fuse(scramble((long)node.hashCode()),fuse(scramble(milliSecs),randomValue)))
                    +"</object>";
        } else {
            return "<object>" + escapeXml(node.toString()) + "</object>";
        }
    }

    private String object(RDFNode node) {
        if (node.isLiteral()) {
            Literal lit = node.asLiteral();
            String text = lit.getString();
            String lang = lit.getLanguage();
            String type = lit.getDatatypeURI();

            if (type == null) {
                type = "http://www.w3.org/2001/XMLSchema#string";
            }

            if (lang == null || "".equals(lang)) {
                lang = "";
            } else {
                lang = " xml:lang='" + lang + "'";
            }

            return "<object datatype='" + escapeXml(type) + "'" + lang + ">" + escapeXml(text) + "</object>";
        } else if (node.isAnon()) {
            return "<object>http://marklogic.com/semantics/blank/" + Long.toHexString(
                    fuse(scramble((long)node.hashCode()),fuse(scramble(milliSecs),randomValue)))
                    +"</object>";
        } else {
            return "<object>" + escapeXml(node.toString()) + "</object>";
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

    @Override
    protected void setKey(String val) {
        if (val == null) {
            key = null;
        } else {
            String uri = getEncodedURI(val) + ".xml";
            super.setKey(uri);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (statementIter == null) {
            return nextStreamingKeyValue();
        } else {
            return nextInMemoryKeyValue();
        }
    }

    public boolean nextInMemoryKeyValue() throws IOException, InterruptedException {
        if (lang == Lang.NQUADS) {
            return nextInMemoryQuadKeyValue();
        } else {
            return nextInMemoryTripleKeyValue();
        }
    }

    public boolean nextInMemoryTripleKeyValue() throws IOException, InterruptedException {
        if (!statementIter.hasNext()) {
            hasNext = false;
            return false;
        }

        setKey(idGen.incrementAndGet());
        write("<triples xmlns='http://marklogic.com/semantics'>");
        int max = MAXTRIPLESPERDOCUMENT;
        while (max > 0 && statementIter.hasNext()) {
            Statement stmt = statementIter.nextStatement();
            write("<triple>");
            write(subject(stmt.getSubject()));
            write(predicate(stmt.getPredicate()));
            write(object(stmt.getObject()));
            write("</triple>");
            max--;
        }
        write("</triples>\n");

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
        while (!statementIter.hasNext()) {
            if (graphNameIter.hasNext()) {
                collection = graphNameIter.next();
                model = dataset.getNamedModel(collection);
                statementIter = model.listStatements();
            } else {
                hasNext = false;
                return false;
            }
        }

        setKey(idGen.incrementAndGet());
        write("<triples xmlns='http://marklogic.com/semantics'>");
        int max = MAXTRIPLESPERDOCUMENT;
        while (max > 0 && statementIter.hasNext()) {
            Statement stmt = statementIter.nextStatement();
            write("<triple>");
            write(subject(stmt.getSubject()));
            write(predicate(stmt.getPredicate()));
            write(object(stmt.getObject()));
            write("</triple>");
            max--;
        }
        write("</triples>\n");

        if (!statementIter.hasNext()) {
            pos = 1;
        }

        writeValue(collection);
        return true;
    }

    public boolean nextInMemoryQuadKeyValueIgnoreCollections() throws IOException, InterruptedException {
        while (!statementIter.hasNext()) {
            if (graphNameIter.hasNext()) {
                collection = graphNameIter.next();
                model = dataset.getNamedModel(collection);
                statementIter = model.listStatements();
            } else {
                hasNext = false;
                return false;
            }
        }

        setKey(idGen.incrementAndGet());
        write("<triples xmlns='http://marklogic.com/semantics'>");
        int max = MAXTRIPLESPERDOCUMENT;
        while (max > 0 && statementIter.hasNext()) {
            Statement stmt = statementIter.nextStatement();
            write("<triple>");
            write(subject(stmt.getSubject()));
            write(predicate(stmt.getPredicate()));
            write(object(stmt.getObject()));
            write("</triple>");
            max--;

            boolean moreTriples = statementIter.hasNext();
            while (!moreTriples) {
                moreTriples = true; // counter-intuitive; get out of the loop if we really are finished
                if (graphNameIter.hasNext()) {
                    collection = graphNameIter.next();
                    model = dataset.getNamedModel(collection);
                    statementIter = model.listStatements();
                    moreTriples = statementIter.hasNext();
                }
            }
        }
        write("</triples>\n");

        if (!statementIter.hasNext()) {
            pos = 1;
        }

        writeValue();
        return true;
    }

    public boolean nextStreamingKeyValue() throws IOException, InterruptedException {
        if (!rdfIter.hasNext() && collectionHash.size() == 0) {
            if(compressed) {
                hasNext = false;
                executor.shutdown();
                return false;
            } else {
                if (iterator!=null && iterator.hasNext()) {
                    close();
                    initStream(iterator.next());
                } else {
                    hasNext = false;
                    executor.shutdown();
                    return false;
                }
            }
        }

        if (lang == Lang.NQUADS) {
            return nextStramingQuadKeyValue();
        } else {
            return nextStreamingTripleKeyValue();
        }
    }

    protected boolean nextStreamingTripleKeyValue() throws IOException, InterruptedException {
        setKey(idGen.incrementAndGet());
        write("<triples xmlns='http://marklogic.com/semantics'>");
        int max = MAXTRIPLESPERDOCUMENT;
        while (max > 0 && rdfIter.hasNext()) {
            Triple triple = (Triple) rdfIter.next();
            write("<triple>");
            write(subject(triple.getSubject()));
            write(predicate(triple.getPredicate()));
            write(object(triple.getObject()));
            write("</triple>");
            max--;
        }
        write("</triples>\n");

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
        setKey(idGen.incrementAndGet());
        write("<triples xmlns='http://marklogic.com/semantics'>");
        int max = MAXTRIPLESPERDOCUMENT;
        while (max > 0 && rdfIter.hasNext()) {
            Quad quad = (Quad) rdfIter.next();
            write("<triple>");
            write(subject(quad.getSubject()));
            write(predicate(quad.getPredicate()));
            write(object(quad.getObject()));
            write("</triple>");
            max--;
        }
        write("</triples>\n");

        if (!rdfIter.hasNext()) {
            pos = 1;
        }

        writeValue();
        return true;
    }

    public boolean nextStreamingQuadKeyValueWithCollections() throws IOException, InterruptedException {
        if (!rdfIter.hasNext() && collectionHash.isEmpty()) {
            hasNext = false;
            executor.shutdown();
            return false;
        }

        String collection = null;
        boolean overflow = false;
        while (!overflow && rdfIter.hasNext()) {
            Quad quad = (Quad) rdfIter.next();

            collection = resource(quad.getGraph());
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

            Vector<String> triples = collectionHash.get(collection);
            triples.add("<triple>" + triple + "</triple>");

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

        if (!overflow) {
            // HACK: fix this!
            for (String c : collectionHash.keySet()) {
                collection = c;
                //System.err.println("Flushing " + collection + " (" + collectionHash.get(collection).size() + ")");
                break;
            }
        }

        Vector<String> triples = collectionHash.get(collection);

        setKey(idGen.incrementAndGet());
        write("<triples xmlns='http://marklogic.com/semantics'>");
        for (String t : triples) {
            write(t);
        }
        write("</triples>\n");

        collectionHash.remove(collection);
        collectionCount--;

        if (!rdfIter.hasNext()) {
            pos = 1;
        }

        writeValue(collection);
        return true;
    }

    public void writeValue() {
        writeValue(null);
    }

    public void writeValue(String collection) {
        if (value instanceof Text) {
            ((Text) value).set(buffer.toString());
        } else if (value instanceof RDFWritable) {
            ((RDFWritable) value).set(buffer.toString());
            if (collection != null) {
                ((RDFWritable) value).setCollection(collection);
            }
        } else if (value instanceof ContentWithFileNameWritable) {
            VALUEIN realValue = ((ContentWithFileNameWritable<VALUEIN>) value)
                    .getValue();
            if (realValue instanceof Text) {
                ((Text) realValue).set(buffer.toString());
            } else {
                LOG.error("Expects RDF, but gets "
                        + realValue.getClass().getCanonicalName());
                key = null;
            }
        } else {
            LOG.error("Expects RDF, but gets "
                    + value.getClass().getCanonicalName());
            key = null;
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
}
