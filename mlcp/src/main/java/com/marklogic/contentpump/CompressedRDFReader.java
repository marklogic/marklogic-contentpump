/*
 * Copyright 2003-2016 MarkLogic Corporation
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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.lang.PipedQuadsStream;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedTriplesStream;

import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import com.marklogic.mapreduce.CompressionCodec;
import com.marklogic.mapreduce.LinkedMapWritable;

/**
 * Reader for Compressed RDF statements.
 * 
 * @author nwalsh
 *
 * @param <VALUEIN>
 */
public class CompressedRDFReader<VALUEIN> extends RDFReader<VALUEIN> {
    public static final Log LOG = LogFactory.getLog(CompressedRDFReader.class);

    // When we're looking at compressed data, for the purposes of deciding if 
    // we should stream or not, we assume it'll be (compressedSize * 
    // COMPRESSIONFACTOR) when it's uncompressed.
    public static final long COMPRESSIONFACTOR = 2;

    private byte[] buf = new byte[65536];
    private InputStream zipIn;
    private ZipEntry currZipEntry;
    private CompressionCodec codec;
    private ExecutorService pool;
    
    @Override
    public void close() throws IOException {
        super.close();
        //close the zip
        if (zipIn != null) {
            zipIn.close();
        }
        if (pool != null) {
            pool.shutdown();
        }
    }

    @Override
    protected void initStream(InputSplit inSplit) throws IOException, InterruptedException {
        setFile(((FileSplit) inSplit).getPath());
        FSDataInputStream fileIn = fs.open(file);
        URI zipURI = file.toUri();
        String codecString = 
                conf.get(ConfigConstants.CONF_INPUT_COMPRESSION_CODEC, 
                        CompressionCodec.ZIP.toString());
        if (codecString.equalsIgnoreCase(CompressionCodec.ZIP.toString())) {
            zipIn = new ZipInputStream(fileIn);
            codec = CompressionCodec.ZIP;
            while (true) {
                try {
                    currZipEntry = ((ZipInputStream)zipIn).getNextEntry();
                    if (currZipEntry == null) {
                        break;
                    }
                    if (currZipEntry.getSize() != 0) {
                        subId = currZipEntry.getName();
                        break;
                    }
                } catch (IllegalArgumentException e) {
                    LOG.warn("Skipped a zip entry in : " + file.toUri()
                            + ", reason: " + e.getMessage());
                }
            }
            if (currZipEntry == null) { // no entry in zip
                LOG.warn("No valid entry in zip:" + file.toUri());
                return;
            }
            ByteArrayOutputStream baos;
            long size = currZipEntry.getSize();
            if (size == -1) {
                baos = new ByteArrayOutputStream();
                // if we don't know the size, assume it's big!
                initParser(zipURI.toASCIIString() + "/" + subId,
                        INMEMORYTHRESHOLD); 
            } else {
                baos = new ByteArrayOutputStream((int) size);
                initParser(zipURI.toASCIIString() + "/" + subId, size);
            }
            int nb;
            while ((nb = zipIn.read(buf, 0, buf.length)) != -1) {
                baos.write(buf, 0, nb);
            }
            parse(subId, new ByteArrayInputStream(baos.toByteArray()));
        } else if (codecString.equalsIgnoreCase(CompressionCodec.GZIP.toString())) {
            long size = inSplit.getLength();
            zipIn = new GZIPInputStream(fileIn);
            codec = CompressionCodec.GZIP;
            initParser(zipURI.toASCIIString(), size * COMPRESSIONFACTOR);
            parse(file.getName(), zipIn);
        } else {
            throw new UnsupportedOperationException("Unsupported codec: " + codec.name());
        }
    }

    protected void parse(String fsname, final InputStream in)
        throws IOException {
        if (dataset == null) {
            if (lang == Lang.NQUADS || lang == Lang.TRIG) {
                rdfIter = new PipedRDFIterator<Quad>();
                @SuppressWarnings("unchecked")
                PipedQuadsStream stream = new PipedQuadsStream(rdfIter);
                rdfInputStream = stream;
            } else {
                rdfIter = new PipedRDFIterator<Triple>();
                @SuppressWarnings("unchecked")
                PipedTriplesStream stream = new PipedTriplesStream(rdfIter);
                rdfInputStream = stream;
            }

            // Create a runnable for our parser thread
            jenaStreamingParser = new RunnableParser(origFn, fsname, in);

            // add it into pool
            pool.submit(jenaStreamingParser);
            // We don't know how many statements are in the model; we could
            // count them, but that's
            // possibly expensive. So we just say 0 until we're done.
            pos = 0;
            end = 1;
        } else {
            loadModel(fsname, in);
        }
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean stillReading = super.nextKeyValue();
        if (stillReading) {
            return true;
        }

        // Ok, we've run out of data in the current file, are there more?
        URI zipURI = file.toUri();
        if (codec.equals(CompressionCodec.ZIP)) {
            ZipInputStream zis = (ZipInputStream) zipIn;

            ByteArrayOutputStream baos;
            while ((currZipEntry = zis.getNextEntry()) != null) {
                if (currZipEntry.getSize() == 0) {
                    continue;
                }

                long size = currZipEntry.getSize();
                if (size == -1) {
                    baos = new ByteArrayOutputStream();
                    // if we don't know the size, assume it's big!
                    initParser(zipURI.toASCIIString() + "/" + currZipEntry.getName(), INMEMORYTHRESHOLD);
                } else {
                    baos = new ByteArrayOutputStream((int) size);
                    initParser(zipURI.toASCIIString() + "/" + currZipEntry.getName(), size);
                }
                int nb;
                while ((nb = zis.read(buf, 0, buf.length)) != -1) {
                    baos.write(buf, 0, nb);
                }

                parse(currZipEntry.getName(), new ByteArrayInputStream(baos.toByteArray()));
                boolean gotTriples = super.nextKeyValue();
                if (gotTriples) {
                    return true;
                }
            }
            // end of zip
            if (iterator != null && iterator.hasNext()) {
                close();
                initStream(iterator.next());
                return super.nextKeyValue();
            }

            return false;
        } else {
            return false;
        }
    }

    public CompressedRDFReader(String version, LinkedMapWritable roleMap) {
        super(version, roleMap);
        compressed = true;
        //allocate a pool of size 1
        pool = Executors.newFixedThreadPool(1);
    }
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return hasNext ? 0 : 1;
    }
    
}
