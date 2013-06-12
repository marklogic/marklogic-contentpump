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

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;
import com.marklogic.contentpump.utilities.IdGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.PipedQuadsStream;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

/**
 * Reader for RDF quads/triples. Uses Jena library to parse RDF and sends triples
 * to the database in groups of MAXTRIPLESPERDOCUMENT.
 * 
 * @author nwalsh
 *
 * @param <VALUEIN>
 */
public class CombinedRDFReader<VALUEIN> extends ImportRecordReader<VALUEIN> {
    public static final Log LOG = LogFactory.getLog(CombinedRDFReader.class);
    public static final int MAXTRIPLESPERDOCUMENT = 100;
    protected static Pattern[] patterns = new Pattern[] {
            Pattern.compile("&"), Pattern.compile("<"), Pattern.compile(">") };

    protected PipedRDFIterator rdfIter;
    protected PipedRDFStream rdfInputStream;
    protected ExecutorService executor;
    protected boolean readQuads;

    protected Hashtable<String, Vector> collectionHash = new Hashtable<String, Vector> ();
    protected int collectionCount = 0;
    private static final int MAX_COLLECTIONS = 100;

    protected StringBuilder buffer;
    protected boolean hasNext = true;
    protected IdGenerator idGen;

    protected Hashtable<String,String> blankMap = new Hashtable<String,String> ();
    protected long randomValue;
    protected long milliSecs;

    protected FileSystem fs;
    protected Path file;
    protected String inputFn; // Tracks input filename even in the CompressedRDFReader case
    protected long splitStart;
    protected long start;
    protected long pos;
    protected long end;

    public CombinedRDFReader() {
        Random random = new Random();
        randomValue = random.nextLong();
        Calendar cal = Calendar.getInstance();
        milliSecs = cal.getTimeInMillis();
    }

    @Override
    public void close() throws IOException {
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
        initConfig(context);

        file = ((FileSplit) inSplit).getPath();
        start = 0;
        pos = 0;
        end = 1;

        configFileNameAsCollection(conf, file);
        fs = file.getFileSystem(context.getConfiguration());
        loadModel(file.getName(), fs.open(file));
        idGen = new IdGenerator(inputFn + "-" + splitStart);
    }

    protected void loadModel(String fsname, final InputStream in) throws IOException {
        String ext = null;
        if (fsname.contains(".")) {
            int pos = fsname.lastIndexOf(".");
            ext = fsname.substring(pos);
            if (".gz".equals(ext)) {
                fsname = fsname.substring(0, pos);
                pos = fsname.lastIndexOf(".");
                ext = fsname.substring(pos);
            }
        }

        inputFn = fsname;
        idGen = new IdGenerator(inputFn + "-" + splitStart);

        if ("nq".equals(ext)) {
            rdfIter = new PipedRDFIterator<Quad>();
            rdfInputStream = new PipedQuadsStream(rdfIter);
            readQuads = true;
        } else {
            rdfIter = new PipedRDFIterator<Triple>();
            rdfInputStream = new PipedTriplesStream(rdfIter);
            readQuads = false;
        }

        // PipedRDFStream and PipedRDFIterator need to be on different threads
        executor = Executors.newSingleThreadExecutor();

        final String baseURI = fsname;

        // Create a runnable for our parser thread
        Runnable parser = new Runnable() {
            @Override
            public void run() {
                // Call the parsing process.
                RDFDataMgr.parse(rdfInputStream, in, baseURI, null, null);
            }
        };

        // Start the parser on another thread
        executor.submit(parser);

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

    protected String resource(Node rsrc) {
        if (rsrc.isBlank()) {
            String uri = null;
            String addr = escapeXml(rsrc.toString());
            if (blankMap.containsKey(addr)) {
                uri = blankMap.get(addr);
            } else {
                uri = "http://marklogic.com/semantics/blank/" + milliSecs + "/" + randomValue + "/" + addr;
                blankMap.put(addr, uri);
            }

            return uri;
        } else {
            return escapeXml(rsrc.toString());
        }
    }

    protected String resource(Node rsrc, String tag) {
        String uri = resource(rsrc);
        return "<" + tag + ">" + uri + "</" + tag + ">";
    }

    protected String subject(Node subj) {
        return resource(subj, "subject");
    }

    protected String predicate(Node subj) {
        return resource(subj, "predicate");
    }

    protected static String object(Node node) {
        if (node.isLiteral()) {
            String lit = node.getLiteralLexicalForm();
            String type = node.getLiteralDatatypeURI();
            if (type == null) {
                type = "http://www.w3.org/2001/XMLSchema#string";
            }
            return "<object datatype='" + type + "'>" + escapeXml(lit) + "</object>";
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
            String uri = getEncodedURI(val);
            super.setKey(uri);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!rdfIter.hasNext()) {
            hasNext = false;
            executor.shutdown();
            return false;
        }

        if (readQuads) {
            return nextQuadKeyValue();
        } else {
            return nextTripleKeyValue();
        }
    }

    protected boolean nextTripleKeyValue() throws IOException, InterruptedException {
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

        if (value instanceof Text) {
            ((Text) value).set(buffer.toString());
        } else if (value instanceof ContentWithFileNameWritable) {
            VALUEIN realValue = ((ContentWithFileNameWritable<VALUEIN>) value)
                    .getValue();
            if (realValue instanceof Text) {
                ((Text) realValue).set(buffer.toString());
            } else {
                LOG.error("Expects Text in triples, but gets "
                        + realValue.getClass().getCanonicalName());
                key = null;
            }
        } else {
            LOG.error("Expects Text in triples, but gets "
                    + value.getClass().getCanonicalName());
            key = null;
        }

        buffer.setLength(0);
        return true;
    }

    public boolean nextQuadKeyValue() throws IOException, InterruptedException {
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

            System.err.println(triple);

            if (triples.size() == MAXTRIPLESPERDOCUMENT) {
                //System.err.println("Full doc " + collection + " (" + triples.size() + ")");
                overflow = true;
            } else if (collectionCount > MAX_COLLECTIONS) {
                collection = largestCollection();
                //System.err.println("Flushing " + collection + " (" + collectionHash.get(collection).size() + ")");
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

        if (value instanceof Text) {
            ((Text) value).set(buffer.toString());
        } else if (value instanceof ContentWithFileNameWritable) {
            VALUEIN realValue = ((ContentWithFileNameWritable<VALUEIN>) value)
                    .getValue();
            if (realValue instanceof Text) {
                ((Text) realValue).set(buffer.toString());
            } else {
                LOG.error("Expects Text in quads, but gets "
                        + realValue.getClass().getCanonicalName());
                key = null;
            }
        } else {
            LOG.error("Expects Text in quads, but gets "
                    + value.getClass().getCanonicalName());
            key = null;
        }

        buffer.setLength(0);
        return true;
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
