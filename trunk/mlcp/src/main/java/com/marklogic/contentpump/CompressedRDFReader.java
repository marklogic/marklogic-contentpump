package com.marklogic.contentpump;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.contentpump.utilities.FileIterator;
import com.marklogic.contentpump.utilities.IdGenerator;
import com.marklogic.mapreduce.CompressionCodec;

/**
 * Reader for Compressed RDF statements.
 * @author nwalsh
 *
 * @param <VALUEIN>
 */
public class CompressedRDFReader<VALUEIN> extends RDFReader<VALUEIN> {
    public static final Log LOG = LogFactory.getLog(CompressedRDFReader.class);

    // When we're looking at compressed data, for the purposes of deciding if we should stream or not,
    // we assume it'll be (compressedSize * COMPRESSIONFACTOR) when it's uncompressed.
    public static final long COMPRESSIONFACTOR = 2;

    private byte[] buf = new byte[65536];
    private InputStream zipIn;
    private ZipEntry currZipEntry;
    private CompressionCodec codec;

    @Override
    public void close() throws IOException {
        super.close();
        //close the zip
        if (zipIn != null) {
            zipIn.close();
        }
    }

/*
    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {

        initConfig(context);

        file = ((FileSplit) inSplit).getPath();
        fs = file.getFileSystem(context.getConfiguration());
        
        FileStatus status = fs.getFileStatus(file);
        if(status.isDir()) {
            iterator = new FileIterator((FileSplit)inSplit, context);
            inSplit = iterator.next();
        }
        
        initStream(inSplit);
    }
*/

    @Override
    protected void initStream(InputSplit inSplit) throws IOException, InterruptedException {
        file = ((FileSplit) inSplit).getPath();
        FSDataInputStream fileIn = fs.open(file);
        URI zipURI = file.toUri();

        String codecString = conf.get(ConfigConstants.CONF_INPUT_COMPRESSION_CODEC, CompressionCodec.ZIP.toString());
        if (codecString.equalsIgnoreCase(CompressionCodec.ZIP.toString())) {
            zipIn = new ZipInputStream(fileIn);
            codec = CompressionCodec.ZIP;

            while ((currZipEntry = ((ZipInputStream) zipIn).getNextEntry()) != null) {
                if (currZipEntry.getSize() != 0) {
                    break;
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
                initParser(zipURI.toASCIIString() + "/" + currZipEntry.getName(), INMEMORYTHRESHOLD); // if we don't know the size, assume it's big!
            } else {
                baos = new ByteArrayOutputStream((int) size);
                initParser(zipURI.toASCIIString() + "/" + currZipEntry.getName(), size);
            }
            int nb;
            while ((nb = zipIn.read(buf, 0, buf.length)) != -1) {
                baos.write(buf, 0, nb);
            }

            parse(currZipEntry.getName(), new ByteArrayInputStream(baos.toByteArray()));
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

    protected void parse(String fsname, final InputStream in) throws IOException {
        loadModel(fsname, in);
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
                return super.nextKeyValue();
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

    public CompressedRDFReader() {
        super();
        compressed = true;
    }
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return hasNext ? 0 : 1;
    }

}
