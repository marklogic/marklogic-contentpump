package com.marklogic.contentpump;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.contentpump.utilities.AggregateXMLInputFactoryImpl;
import com.marklogic.mapreduce.CompressionCodec;
import com.sun.org.apache.xerces.internal.xni.NamespaceContext;

public class CompressedAggXMLReader<VALUEIN> extends
    AggregateXMLReader<VALUEIN> {
    private byte[] buf = new byte[65536];
    private InputStream zipIn;
    private XMLInputFactory factory;
    private ZipEntry currZipEntry;
    private CompressionCodec codec;

    @Override
    public void close() throws IOException {
        super.close();
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path file = ((FileSplit) inSplit).getPath();
        initCommonConfigurations(conf, file);
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        FSDataInputStream fileIn = fs.open(file);
        factory = new AggregateXMLInputFactoryImpl();

        String codecString = conf.get(
            ConfigConstants.CONF_INPUT_COMPRESSION_CODEC,
            CompressionCodec.ZIP.toString());
        if (codecString.equalsIgnoreCase(CompressionCodec.ZIP.toString())) {
            zipIn = new ZipInputStream(fileIn);
            codec = CompressionCodec.ZIP;
            while ((currZipEntry = ((ZipInputStream) zipIn).getNextEntry()) != null) {
                if (!currZipEntry.isDirectory() && currZipEntry.getSize() != 0) {
                    break;
                }
            }
            if (currZipEntry == null) { // no entry in zip
                return;
            }
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            long size;
            while ((size = zipIn.read(buf, 0, buf.length)) != -1) {
                baos.write(buf, 0, (int) size);
            }
            try {
                xmlSR = factory
                    .createXMLStreamReader(new ByteArrayInputStream(baos
                        .toByteArray()));
            } catch (XMLStreamException e) {
                e.printStackTrace();
            }

        } else if (codecString.equalsIgnoreCase(CompressionCodec.GZIP
            .toString())) {
            zipIn = new GZIPInputStream(fileIn);
            codec = CompressionCodec.GZIP;
            try {
                xmlSR = factory.createXMLStreamReader(zipIn);
            } catch (XMLStreamException e) {
                e.printStackTrace();
            }
        }

        initAggConf(inSplit, context);
    }

    private boolean nextRecordInAggregate() throws IOException,
        XMLStreamException, InterruptedException {
        return super.nextKeyValue();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (zipIn == null) {
            hasNext = false;
            return false;
        }
        try {
            if (codec.equals(CompressionCodec.ZIP)) {
                ZipInputStream zis = (ZipInputStream) zipIn;

                if (xmlSR.hasNext()) {
                    hasNext = nextRecordInAggregate();
                    if (hasNext) {
                        return true;
                    }
                }
                // xmlSR does not hasNext, try next zipEntry if any
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                while ((currZipEntry = zis.getNextEntry()) != null) {
                    if (currZipEntry.isDirectory()
                        || currZipEntry.getSize() == 0) {
                        continue;
                    }
                    long size;
                    while ((size = zis.read(buf, 0, buf.length)) != -1) {
                        baos.write(buf, 0, (int) size);
                    }
                    xmlSR = factory
                        .createXMLStreamReader(new ByteArrayInputStream(baos
                            .toByteArray()));
                    nameSpaces.clear();

                    return nextRecordInAggregate();
                }
                // end of zip
                return false;

            } else if (codec.equals(CompressionCodec.GZIP)) {
                return nextRecordInAggregate();
            }
        } catch (XMLStreamException e1) {
            e1.printStackTrace();
        }
        return true;
    }
    public CompressedAggXMLReader(String recordElem,
        String namespace, NamespaceContext nsctx) {
        super(recordElem, namespace, nsctx);
    }

}
