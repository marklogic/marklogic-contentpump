package com.marklogic.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.marklogic.io.BiendianDataInputStream;
import com.marklogic.tree.CompressedTreeDecoder;
import com.marklogic.tree.ExpandedTree;

public class ForestReader<VALUEIN> extends RecordReader<DocumentURI, VALUEIN>
        implements MarkLogicConstants {
    public static final Log LOG = LogFactory.getLog(ForestReader.class);
    protected FileSplit split;
    protected long bytesRead;
    protected Configuration conf;
    protected BiendianDataInputStream dataIs;
    protected BiendianDataInputStream ordIs;
    protected BiendianDataInputStream tsIs;
    protected DocumentURI key;
    protected VALUEIN value;
    protected Class<? extends Writable> valueClass;
    protected int position;
    protected int prevDocid = -1;
    protected boolean done = false;
    protected Path largeForestDir;
    protected int nascentCnt = 0;
    protected int deletedCnt = 0;
    protected int fragCnt = 0;

    @Override
    public void close() throws IOException {
        if (dataIs != null) {
            dataIs.close();
        }
        if (ordIs != null) {
            ordIs.close();
        }
        if (tsIs != null) {
            tsIs.close();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Nascent count: " + nascentCnt + 
                    " Deleted count: " + deletedCnt + " Bytes read = " +
                    bytesRead + " Fragment count: " + fragCnt +
                    " Last docid: " + prevDocid);
        }
    }

    @Override
    public DocumentURI getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return done ? 1 : bytesRead / (float) split.getLength();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        this.split = (FileSplit) split;
        conf = context.getConfiguration();
        Path dataPath = this.split.getPath();
        FileSystem fs = dataPath.getFileSystem(conf);
        dataIs = new BiendianDataInputStream(fs.open(dataPath));
        dataIs.skipBytes(this.split.getStart());
        Path ordPath = new Path(dataPath.getParent(), "Ordinals");
        ordIs = new BiendianDataInputStream(fs.open(ordPath));
        Path tsPath = new Path(dataPath.getParent(), "Timestamps");
        tsIs = new BiendianDataInputStream(fs.open(tsPath));
        valueClass = conf.getClass(INPUT_VALUE_CLASS, ForestDocument.class, 
                               Writable.class);
        if (!ForestDocument.class.isAssignableFrom(valueClass)) {
            throw new IllegalArgumentException("Unsupported " + 
                    INPUT_VALUE_CLASS);
        }
        largeForestDir = new Path(dataPath.getParent().getParent(), "Large");
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (bytesRead < split.getLength() && !done) {
            ExpandedTree tree = getNextTree();
            if (tree == null) {
                continue;
            }
            String uri = tree.getDocumentURI();
            key = new DocumentURI(uri);
            value = (VALUEIN) ForestDocument.createDocument(conf, 
                    largeForestDir, tree, uri);
            if (value != null) return true;
        }
        return false;
    }

    private ExpandedTree getNextTree() throws IOException {
        int j;
        try {
            int docid = dataIs.readInt();
            int csword = dataIs.readInt();
            int fdatw = dataIs.readInt();
            bytesRead += 12;
            int datWords = csword & 0x0000000f;
            int hdrWords = 2;
            if (datWords == 0) {
                datWords = fdatw;
                hdrWords = 3;
                LOG.trace("3 header words");
            }
            if (docid == 0xffffffff && csword == 0xffffffff
                    && fdatw == 0xffffffff) {
                LOG.trace("Reached the end");
                done = true;
                return null;
            }
            if (prevDocid != -1 && docid <= prevDocid) {
                throw new RuntimeException("docid out of order, postition = "
                        + position + ", docid = " + docid + ", prevDocid = "
                        + prevDocid);
            }
            if (prevDocid == -1 && docid != 0) { // need to skip tsIs and ordIs
                ordIs.skipBytes(docid * 8);
                tsIs.skipBytes(docid * 8 * 2);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("First docid: " + docid);
                }
            } else {
                int docidGap = docid - prevDocid;
                if (docidGap > 1) {
                    ordIs.skipBytes(docidGap - 1);
                    tsIs.skipBytes(docidGap - 1);
                }
            }
            prevDocid = docid;
            if (hdrWords == 2) {
                j = datWords - 1;
            } else {
                j = datWords;
            }
            j *= 4;
            fragCnt++;
            long nascent = tsIs.readLong();
            long deleted = tsIs.readLong();
            
            if (LOG.isTraceEnabled()) {
                LOG.trace(String.format("TreeData p %d d %d w %d nt %d dt %d",
                        position, docid, datWords, nascent, deleted));
            }
            
            if (nascent == 0L || deleted != -1L) { // skip
                position++;
                bytesRead += dataIs.skipBytes(j);
                if (nascent == 0L) nascentCnt++;
                if (deleted != -1L) deletedCnt++;
                return null;
            }
        } catch (EOFException e) {
            done = true;
            return null;
        }
       
        // TODO: Is it better to read into a buffer or directly from the 
        // stream then reset and skip?
        byte[] buf = new byte[j];  
        InputStream in = dataIs.getInputStream();
        for (int read = 0; read < j; ) {
            read += in.read(buf, read, j - read);
        } 
        bytesRead += j;
        position++;
        ByteArrayInputStream bis = new ByteArrayInputStream(buf);
        BiendianDataInputStream is = new BiendianDataInputStream(bis);
        ExpandedTree tree = new CompressedTreeDecoder().decode(is);
        return tree;
    }
}
