/*
 * Copyright 2003-2014 MarkLogic Corporation
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
package com.marklogic.io;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.InputStream;
import java.io.IOException;

/**
 * InputStream able to handle both big-endian and little-endian data format.
 * 
 * @author jchen
 */
public class BiendianDataInputStream extends InputStream implements DataInput {

    private final InputStream in;
    private final DataInputStream di;
    private final byte buf[] = new byte[8];

    private boolean littleEndian = true;

    public BiendianDataInputStream(InputStream in) {
        this.in = in;
        this.di = new DataInputStream(in);
    }
    
    public InputStream getInputStream() {
        return in;
    }
    
    public final void setLittleEndian(boolean littleEndian) {
        this.littleEndian = littleEndian;
    }
    
    public final boolean isLittleEndian() {
        return littleEndian;
    }

    public final int read(byte b[]) throws IOException {
        return di.read(b, 0, b.length);
    }

    public final int read(byte b[], int off, int len) throws IOException {
        return di.read(b, off, len);
    }

    public final void readFully(byte b[]) throws IOException {
        readFully(b, 0, b.length);
    }

    public final void readFully(byte b[], int off, int len) throws IOException {
        di.readFully(b, off, len);
    }

    public final int skipBytes(int n) throws IOException {
        return di.skipBytes(n);
    }
    
    public final long skipBytes(long n) throws IOException {
        long total = 0;
        while (n > Integer.MAX_VALUE) {
            int skipped = di.skipBytes(Integer.MAX_VALUE);
            n -= skipped;
            total += skipped;
        }  
        return total + di.skipBytes((int)n);
    }

    public final boolean readBoolean() throws IOException {
        return di.readBoolean();
    }

    public final byte readByte() throws IOException {
        return di.readByte();
    }

    public final int readUnsignedByte() throws IOException {
        return di.readUnsignedByte();
    }

    public final short readShort() throws IOException {
        if (littleEndian) {
            final int a = in.read();
            final int b = in.read();
            if ((a | b) < 0)
                throw new EOFException();
            return (short)((b << 8) + (a << 0));
        }
        else {
            return di.readShort();
        }
    }

    public final int readUnsignedShort() throws IOException {
        if (littleEndian) {
            final int a = in.read();
            final int b = in.read();
            if ((a | b) < 0)
                throw new EOFException();
            return (b << 8) + (a << 0);
        } else {
            return di.readUnsignedShort();
        }
    }

    public final char readChar() throws IOException {
        if (littleEndian) {
            final int a = in.read();
            final int b = in.read();
            if ((a | b) < 0)
                throw new EOFException();
            return (char)((b << 8) + (a << 0));
        } else {
            return di.readChar();
        }
    }

    public final int readInt() throws IOException {
        di.readFully(buf, 0, 4);
        if (littleEndian) {
            return (((buf[3] & 255) << 24) + ((buf[2] & 255) << 16) + 
                    ((buf[1] & 255) << 8) + ((buf[0] & 255) << 0));
        } else {
            return (((buf[0] & 255) << 24) + ((buf[1] & 255) << 16) + 
                    ((buf[2] & 255) << 8) + ((buf[3] & 255) << 0));
        }
    }

    public final long readLong() throws IOException {
        if (littleEndian) {
            di.readFully(buf, 0, 8);
            return (((long)buf[7] << 56) + ((long)(buf[6] & 255) << 48) 
                    + ((long)(buf[5] & 255) << 40)
                    + ((long)(buf[4] & 255) << 32) 
                    + ((long)(buf[3] & 255) << 24) + ((buf[2] & 255) << 16)
                    + ((buf[1] & 255) << 8) + ((buf[0] & 255) << 0));
        } else {
            return di.readLong();
        }
    }

    public final float readFloat() throws IOException {
        return di.readFloat();
    }

    public final double readDouble() throws IOException {
        return di.readDouble();
    }

    @SuppressWarnings("deprecation")
    public final String readLine() throws IOException {
        return di.readLine();
    }

    public final String readUTF() throws IOException {
        return DataInputStream.readUTF(this);
    }

    public final static String readUTF(DataInput in) throws IOException {
        return DataInputStream.readUTF(in);
    }

    @Override
    public int read() throws IOException {
        return di.read();
    }
    
    @Override
    public void close() throws IOException {
        in.close();
    }
}
