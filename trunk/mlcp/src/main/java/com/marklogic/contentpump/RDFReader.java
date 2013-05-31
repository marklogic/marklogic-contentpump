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
import com.marklogic.contentpump.utilities.IdGenerator;
import com.marklogic.contentpump.utilities.IdGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;
import java.util.Calendar;
import java.util.Hashtable;
import java.util.Random;
import java.util.regex.Pattern;

/**
 * Reader for RDF quads/triples. Uses Jena library to parse RDF and sends triples
 * to the database in groups of MAXTRIPLESPERDOCUMENT.
 * 
 * @author nwalsh
 *
 * @param <VALUEIN>
 */
public abstract class RDFReader<VALUEIN> extends ImportRecordReader<VALUEIN> {
    public static final Log LOG = LogFactory.getLog(TriplesReader.class);
    public static final int MAXTRIPLESPERDOCUMENT = 100;
    protected static Pattern[] patterns = new Pattern[] {
            Pattern.compile("&"), Pattern.compile("<"), Pattern.compile(">") };

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

    public RDFReader() {
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

    protected abstract void loadModel(String fsname, final InputStream in) throws IOException;

    protected void write(String str) {
        if (buffer == null) {
            buffer = new StringBuilder();
        }
        buffer.append(str);
    }

    protected String resource(Node rsrc, String tag) {
        if (rsrc.isBlank()) {
            String uri = null;
            String addr = escapeXml(rsrc.toString());
            if (blankMap.containsKey(addr)) {
                uri = blankMap.get(addr);
            } else {
                uri = "http://marklogic.com/semantics/blank/" + milliSecs + "/" + randomValue + "/" + addr;
                blankMap.put(addr, uri);
            }

            return "<" + tag + ">" + uri + "</" + tag + ">";
        } else {
            return "<" + tag + ">" + escapeXml(rsrc.toString()) + "</" + tag + ">";
        }
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
}
