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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class CompressedAggregateXMLReader extends AggregateXMLReader<Text> {
    private byte[] buf = new byte[65536];
    private InputStream zipIn;
    private XMLInputFactory factory;
    private ZipEntry currZipEntry;
    private CompressionCodecEnum codec;

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
        System.err.println(file.toUri());
        factory = XMLInputFactory.newInstance();

        String codecString = conf.get(
            ConfigConstants.CONF_INPUT_COMPRESSION_CODEC,
            CompressionCodecEnum.ZIP.toString());
        if (codecString.equalsIgnoreCase(CompressionCodecEnum.ZIP.toString())) {
            zipIn = new ZipInputStream(fileIn);
            codec = CompressionCodecEnum.ZIP;
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
                // skip the root element
                xmlSR.next();
            } catch (XMLStreamException e) {
                e.printStackTrace();
            }

        } else if (codecString.equalsIgnoreCase(CompressionCodecEnum.GZIP
            .toString())) {
            zipIn = new GZIPInputStream(fileIn);
            codec = CompressionCodecEnum.GZIP;
            try {
                xmlSR = factory.createXMLStreamReader(zipIn);
                // skip the root element
                xmlSR.next();
            } catch (XMLStreamException e) {
                e.printStackTrace();
            }
        }
        // copy the namespaces declared in root element
        copyNameSpaceDecl();

        idName = conf.get(ConfigConstants.CONF_AGGREGATE_URI_ID);
        if(idName == null) {
            useAutomaticId = true;
        }
        recordName = conf.get(ConfigConstants.CONF_AGGREGATE_RECORD_ELEMENT);
        recordNamespace = conf
            .get(ConfigConstants.CONF_AGGREGATE_RECORD_NAMESPACE);
        mode = conf.get(ConfigConstants.CONF_MODE);
        if (mode == null) {
            idGen = new LocalIdGenerator(context.getTaskAttemptID().toString());
        } else if (mode.equals(ConfigConstants.MODE_LOCAL)) {
            idGen = new LocalIdGenerator(file.getName());
        }
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
            if (codec.equals(CompressionCodecEnum.ZIP)) {
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
                    System.err.println(currZipEntry.getName());
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
                    // skip the root element
                    xmlSR.next();
                    nameSpaces.clear();
                    copyNameSpaceDecl();

                    return nextRecordInAggregate();
                }
                // end of zip
                return false;

            } else if (codec.equals(CompressionCodecEnum.GZIP)) {
                return nextRecordInAggregate();
            }
        } catch (XMLStreamException e1) {
            e1.printStackTrace();
        }
        return true;
    }

}
