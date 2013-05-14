package com.marklogic.contentpump;

import com.marklogic.contentpump.utilities.LocalIdGenerator;
import com.marklogic.mapreduce.CompressionCodec;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Reader for Compressed RDF statements.
 * @author nwalsh
 *
 * @param <VALUEIN>
 */
public class CompressedTriplesReader<VALUEIN> extends
        TriplesReader<VALUEIN> {
    public static final Log LOG = LogFactory
        .getLog(CompressedTriplesReader.class);
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

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {

        initConfig(context);

        Path file = ((FileSplit) inSplit).getPath();
        fs = file.getFileSystem(context.getConfiguration());
        FSDataInputStream fileIn = fs.open(file);

        String codecString = conf.get(ConfigConstants.CONF_INPUT_COMPRESSION_CODEC,
                CompressionCodec.ZIP.toString());
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
            } else {
                baos = new ByteArrayOutputStream((int) size);
            }
            int nb;
            while ((nb = zipIn.read(buf, 0, buf.length)) != -1) {
                baos.write(buf, 0, nb);
            }

            loadModel(currZipEntry.getName(), new ByteArrayInputStream(baos.toByteArray()));
        } else if (codecString.equalsIgnoreCase(CompressionCodec.GZIP.toString())) {
            zipIn = new GZIPInputStream(fileIn);
            codec = CompressionCodec.GZIP;

            loadModel(file.getName(), zipIn);
        } else {
            throw new UnsupportedOperationException("Unsupported codec: "
                + codec.name());
        }

        idGen = new LocalIdGenerator(inputFn + "-" + splitStart);
    }

    private boolean nextRecordInAggregate() throws IOException, InterruptedException {
        return super.nextKeyValue();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (zipIn == null || rdfIter == null) {
            hasNext = false;
            return false;
        }

        if (rdfIter.hasNext()) {
            hasNext = nextRecordInAggregate();
            if (hasNext) {
                return true;
            }
        }

        if (codec.equals(CompressionCodec.ZIP)) {
            ZipInputStream zis = (ZipInputStream) zipIn;

            // rdfIter has run out of statements.
            // If there is next zipEntry, build a new model and iterate over it
            ByteArrayOutputStream baos;
            while ((currZipEntry = zis.getNextEntry()) != null) {
                if (currZipEntry.getSize() == 0) {
                    continue;
                }

                long size = currZipEntry.getSize();
                if (size == -1) {
                    baos = new ByteArrayOutputStream();
                } else {
                    baos = new ByteArrayOutputStream((int) size);
                }
                int nb;
                while ((nb = zis.read(buf, 0, buf.length)) != -1) {
                    baos.write(buf, 0, nb);
                }

                loadModel(currZipEntry.getName(), new ByteArrayInputStream(baos.toByteArray()));
                return nextRecordInAggregate();
            }
            // end of zip
            return false;

        } else if (codec.equals(CompressionCodec.GZIP)) {
            return nextRecordInAggregate();
        }
        return true;
    }
    public CompressedTriplesReader() {
        super();
    }
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return hasNext ? 0 : 1;
    }

}
