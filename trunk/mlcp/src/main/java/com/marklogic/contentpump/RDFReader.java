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

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.marklogic.contentpump.utilities.IdGenerator;
import com.marklogic.contentpump.utilities.LocalIdGenerator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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
 * Reader for RDF statements. Uses Jena library to parse RDF and sends triples
 * to the database in groups of MAXTRIPLESPERDOCUMENT.
 * 
 * @author nwalsh
 *
 * @param <VALUEIN>
 */
public class RDFReader<VALUEIN> extends ImportRecordReader<VALUEIN> {
    public static final Log LOG = LogFactory.getLog(RDFReader.class);
    public static final int MAXTRIPLESPERDOCUMENT = 100;
    protected static Pattern[] patterns = new Pattern[] {
        Pattern.compile("&"), Pattern.compile("<"), Pattern.compile(">") };
    protected Model model;
    protected StmtIterator statementIter;

    private StringBuilder buffer;
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
        model.close();
        statementIter.close();
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
        idGen = new LocalIdGenerator(inputFn + "-" + splitStart);
    }

    protected void loadModel(String fsname, InputStream in) throws IOException {
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
        idGen = new LocalIdGenerator(inputFn + "-" + splitStart);

        String type = "RDF/XML";
        if (".n3".equals(ext)) {
            type = "N3";
        } else if (".ttl".equals(ext)) {
            type = "TURTLE";
        } else if (".nt".equals(ext)) {
            type = "N-TRIPLE";
        }

        if (model != null && !model.isClosed()) {
            model.close();
            statementIter.close();
        }

        model = ModelFactory.createDefaultModel();
        model.read(in, null, type);

        in.close();

        statementIter = model.listStatements();

        // We don't know how many statements are in the model; we could count them, but that's
        // possibly expensive. So we just say 0 until we're done.
        pos = 0;
        end = 1;
    }

    private void write(String str) {
        if (buffer == null) {
            buffer = new StringBuilder();
        }
        buffer.append(str);
    }

    private String resource(Resource rsrc, String tag) {
        if (rsrc.isAnon()) {
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

    protected String subject(Resource subj) {
        return resource(subj, "subject");
    }

    protected String predicate(Resource subj) {
        return resource(subj, "predicate");
    }

    private static String object(RDFNode node) {
        if (node.isLiteral()) {
            String lit = node.toString();
            String type = "http://www.w3.org/2001/XMLSchema#string";
            if (lit.indexOf("^^") > 0) {
                type = lit.substring(lit.indexOf("^^") + 2);
                lit = lit.substring(0, lit.indexOf("^^"));
            }
            return "<object datatype='" + type + "'>" + escapeXml(lit) + "</object>";
        } else {
            return "<object>" + escapeXml(node.toString()) + "</object>";
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
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

        if (value instanceof Text) {
            ((Text) value).set(buffer.toString());
        } else if (value instanceof ContentWithFileNameWritable) {
            VALUEIN realValue = ((ContentWithFileNameWritable<VALUEIN>) value)
                    .getValue();
            if (realValue instanceof Text) {
                ((Text) realValue).set(buffer.toString());
            } else {
                LOG.error("Expects Text in RDF, but gets "
                        + realValue.getClass().getCanonicalName());
                key = null;
            }
        } else {
            LOG.error("Expects Text in RDF, but gets "
                    + value.getClass().getCanonicalName());
            key = null;
        }

        buffer.setLength(0);
        return true;
    }
    
    public static String escapeXml(String _in) {
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
