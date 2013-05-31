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

import com.hp.hpl.jena.sparql.core.Quad;
import com.marklogic.contentpump.utilities.IdGenerator;
import org.apache.hadoop.io.Text;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.PipedQuadsStream;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Reader for RDF quads. Uses Jena library to parse RDF and sends triples
 * to the database in groups of MAXTRIPLESPERDOCUMENT.
 * 
 * @author nwalsh
 *
 * @param <VALUEIN>
 */
public class QuadsReader<VALUEIN> extends RDFReader<VALUEIN> {
    protected PipedRDFIterator<Quad> rdfIter;
    protected PipedRDFStream<Quad> rdfInputStream;
    protected ExecutorService executor;

    public QuadsReader() {
        super();
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

        rdfIter = new PipedRDFIterator<Quad>();
        rdfInputStream = new PipedQuadsStream(rdfIter);

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


    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!rdfIter.hasNext()) {
            hasNext = false;
            executor.shutdown();
            return false;
        }

        setKey(idGen.incrementAndGet());
        write("<triples xmlns='http://marklogic.com/semantics'>");
        int max = MAXTRIPLESPERDOCUMENT;
        while (max > 0 && rdfIter.hasNext()) {
            Quad quad = rdfIter.next();
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
}
