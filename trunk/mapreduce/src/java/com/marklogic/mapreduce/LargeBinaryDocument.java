/*
 * Copyright 2003-2015 MarkLogic Corporation
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
package com.marklogic.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import com.marklogic.tree.ExpandedTree;

/**
 * A {@link BinaryDocument} representing a large binary document 
 * extracted from a forest using Direct Access. 
 * 
 * <p>
 * A large binary is stored outside of a MarkLogic fragment. 
 * A binary document categorized as "large" when it exceeds the large
 * size threshold configured for a database. For more details, see
 * "Working With Binary Documents" in the MarkLogic Server
 * <em>Application Developer's Guide</em>.
 * </p>
 * 
 * @see ForestInputFormat
 * @author jchen
 */
public class LargeBinaryDocument extends BinaryDocument {
    public static final Log LOG = LogFactory.getLog(
            LargeBinaryDocument.class);
    protected Path path;
    protected long offset;
    protected long size;
    protected long binaryOrigLen;
    protected Configuration conf;
    
    public LargeBinaryDocument() {
    }
    
    public LargeBinaryDocument(Configuration conf, Path forestDir, 
            ExpandedTree tree) {
        path = new Path(forestDir, tree.getPathToBinary());
        offset = tree.binaryOffset;
        size = tree.binarySize;
        binaryOrigLen = tree.binaryOrigLen;
        this.conf = conf;
        if (LOG.isTraceEnabled()) {
            LOG.trace("Large binary path: " + path);
            LOG.trace("offset: " + offset);
            LOG.trace("size: " + size);
            LOG.trace("origLen: " + binaryOrigLen);
        }
    }
    
    public Path getPath() {
        return path;
    }

    public long getOffset() {
        return offset;
    }

    public long getBinaryOrigLen() {
        return binaryOrigLen;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        path = new Path(Text.readString(in));
        offset = in.readLong();
        size = in.readLong();
        binaryOrigLen = in.readLong();
        conf = new Configuration();
        conf.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        Text.writeString(out, path.toString());
        out.writeLong(offset);
        out.writeLong(size);
        out.writeLong(binaryOrigLen);
        conf.write(out);
    }
    
    @Override
    public byte[] getContentAsByteArray() {
        if (size > Integer.MAX_VALUE) {
            throw new ArrayIndexOutOfBoundsException("Array size = " + size);
        }
        return getContentAsByteArray(0, (int)size);  
    }
    
    public byte[] getContentAsByteArray(int offset, int len) {
        FileSystem fs;
        FSDataInputStream is = null;
        try {
            fs = path.getFileSystem(conf);
            if (!fs.exists(path)) {
                throw new RuntimeException("File not found: " + path);
            }
            FileStatus status = fs.getFileStatus(path);
            if (status.getLen() < offset) {
                throw new RuntimeException("Reached end of file: " + path);
            }
            byte[] buf = new byte[(int) len];
            is = fs.open(path);
            for (int toSkip = offset, skipped = 0; 
                 toSkip < offset; 
                 toSkip -= skipped) {
                skipped = is.skipBytes(offset);
            }
            for (int bytesRead = 0; bytesRead < len;) {
                bytesRead += is.read(buf, bytesRead, len - bytesRead);
            }
            return buf;
        } catch (IOException e) {
            throw new RuntimeException("Error accessing file: " + path, e);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (IOException e) {
                }
            }
        }
    }

    @Override
    public MarkLogicNode getContentAsMarkLogicNode() {
        throw new UnsupportedOperationException(
        "Cannot convert binary data to MarkLogicNode.");
    }

    @Override
    public Text getContentAsText() {
        throw new UnsupportedOperationException(
        "Cannot convert binary data to Text.");
    }

    @Override
    public ContentType getContentType() {
        return ContentType.BINARY;
    }

    @Override
    public String getContentAsString() throws UnsupportedEncodingException {
        throw new UnsupportedOperationException(
                "Cannot convert binary data to String.");
    }

    @Override
    public InputStream getContentAsByteStream() {
        FileSystem fs;
        FSDataInputStream is = null;
        try {
            fs = path.getFileSystem(conf);
            if (!fs.exists(path)) {
                throw new RuntimeException("File not found: " + path);
            }
            is = fs.open(path);
            return is;
        } catch (IOException e) {
            throw new RuntimeException("Error accessing file: " + path, e);
        }
    }

    @Override
    public long getContentSize() {
        return size;
    }
    
    @Override
    public boolean isStreamable() {
        return true;
    }
}
