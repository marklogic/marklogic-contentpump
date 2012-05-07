package com.marklogic.contentpump;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
    private ZipInputStream zipIn;
    private XMLInputFactory factory;
    private ZipEntry currZipEntry;
    @Override
    public void close() throws IOException {
        super.close();
//        zfile.close();
    }


    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
        throws IOException, InterruptedException {
        initCommonConfigurations(context);
        Path file = ((FileSplit) inSplit).getPath();
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        FSDataInputStream fileIn = fs.open(file);
        System.err.println(file.toUri());
//        System.err.println(file.toUri());
//        ZipFile zfile = new ZipFile(new File(file.toUri()));
//        System.err.println(zfile.toString());
        
        zipIn = new ZipInputStream(fileIn);
        while ((currZipEntry = zipIn.getNextEntry()) != null) {
            if (!currZipEntry.isDirectory() && currZipEntry.getSize() != 0) {
                break;
            }
        }
        if(currZipEntry == null) { // no entry in zip
            return;
        }
        Configuration conf = context.getConfiguration();
        // TODO: support codec
        String codec = conf.get(ConfigConstants.CONF_INPUT_COMPRESSION_CODEC);

        factory = XMLInputFactory.newInstance();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        long size;
        while ((size = zipIn.read(buf, 0, buf.length)) != -1) {
            baos.write(buf, 0, (int) size);
        }
        try {
            xmlSR = factory.createXMLStreamReader(new ByteArrayInputStream(baos.toByteArray()));
            // skip the root element
            xmlSR.next();
            //copy the namespaces declared in root element
            copyNameSpaceDecl();
        } catch (XMLStreamException e) {
            e.printStackTrace();
        }
        idName = conf.get(ConfigConstants.CONF_AGGREGATE_URI_ID);
        recordName = conf.get(ConfigConstants.CONF_AGGREGATE_RECORD_ELEMENT);
        recordNamespace = conf
            .get(ConfigConstants.CONF_AGGREGATE_RECORD_NAMESPACE);
    }

    private boolean nextRecordInAggregate() throws IOException, XMLStreamException, InterruptedException{
        return super.nextKeyValue();
    }
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (zipIn == null) {
            hasNext = false;
            return false;
        }
        
        try {
            if(xmlSR.hasNext()){
                hasNext= nextRecordInAggregate();
                if(hasNext) {
                    return true;
                }
            }
            //xmlSR does not hasNext, try next zipEntry if any
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            while ((currZipEntry = zipIn.getNextEntry()) != null) {
                System.err.println(currZipEntry.getName());
                if ( currZipEntry.isDirectory() || currZipEntry.getSize() ==0) {
                    continue;
                }
                long size;
                while ((size = zipIn.read(buf, 0, buf.length)) != -1) {
                    baos.write(buf, 0, (int) size);
                }
                xmlSR = factory.createXMLStreamReader(new ByteArrayInputStream(baos.toByteArray()));
                // skip the root element
                xmlSR.next();
                nameSpaces.clear();
                copyNameSpaceDecl();

                return nextRecordInAggregate();
            }
            //end of zip
            return false;
        } catch (XMLStreamException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        return true;
    }

}
