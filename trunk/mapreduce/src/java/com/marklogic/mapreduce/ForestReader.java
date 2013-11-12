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
    protected BiendianDataInputStream is;
    protected InputStream in;
    protected DocumentURI key;
    protected VALUEIN value;
    protected Class<? extends Writable> valueClass;
    protected int position = 0, prevDocid = -1;
    protected boolean done = false;
    protected Path largeForestDir;

    @Override
    public void close() throws IOException {
        if (is != null) {
            is.close();
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
        Path path = this.split.getPath();
        FileSystem fs = path.getFileSystem(conf);
        in = fs.open(path);
        is = new BiendianDataInputStream(in);
        valueClass = conf.getClass(INPUT_VALUE_CLASS, UnpackedDocument.class, 
                Writable.class);
        if (!UnpackedDocument.class.isAssignableFrom(valueClass)) {
            throw new IllegalArgumentException("Unsupported " + 
                    INPUT_VALUE_CLASS);
        }
        largeForestDir = new Path(path.getParent().getParent(), "Large");
    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        while (!done) {
            ExpandedTree tree = getNextTree();
            if (tree == null) {
                done = true;
                return false;
            }
            String uri = tree.getDocumentURI();
            key = new DocumentURI(uri);
            value = (VALUEIN) UnpackedDocument.createDocument(conf, 
                    largeForestDir, tree, uri);
            if (value != null) return true;
        }
        return false;
    }

    private ExpandedTree getNextTree() throws IOException {
        int j;
        try {
            int docid = is.readInt();
            int csword = is.readInt();
            int fdatw = is.readInt();
            bytesRead += 96;
            int checksum = csword & 0xfffffff0;
            int datWords = csword & 0x0000000f;
            int hdrWords = 2;
            if (datWords == 0) {
                datWords = fdatw;
                hdrWords = 3;
                LOG.trace("3 header words");
            }
            if (docid == 0xffffffff && csword == 0xffffffff
                    && fdatw == 0xffffffff) {
                done = true;
                return null;
            }
            if (prevDocid != -1 && docid <= prevDocid) {
                throw new RuntimeException("docid out of order, postition = "
                        + position + ", docid = " + docid + ", prevDocid = "
                        + prevDocid);
            }
            prevDocid = docid;
            if (hdrWords == 2) {
                j = datWords - 1;
            } else {
                j = datWords;
            }
            j *= 4;

            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("TreeData p %d d %d w %d",
                        position, docid, datWords));
            }
        } catch (EOFException e) {
            done = true;
            return null;
        }
        byte[] buf = new byte[j];       
        for (int bytesRead = 0; bytesRead < j; ) {
            bytesRead += in.read(buf, bytesRead, j - bytesRead);
        }
        bytesRead += j;
        position++;
        ByteArrayInputStream bis = new ByteArrayInputStream(buf);
        BiendianDataInputStream is = new BiendianDataInputStream(bis);
        ExpandedTree tree = new CompressedTreeDecoder().decode(is);
        return tree;
    }
}
