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
package com.marklogic.io;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Decoder used to decode compressed tree data.
 * 
 * @author jchen
 */
public class Decoder {
    public static final Log LOG = LogFactory.getLog(Decoder.class);
    
    DataInput in;

    int numBitsInReg = 0;
    public long reg = 0;

    public Decoder(DataInput in) {
        this.in = in;
    }

    private void load() throws IOException {
        try {
            long bits = in.readInt() & 0xffffffffL;
            reg |= bits << numBitsInReg;
            numBitsInReg += 32;
        } catch (EOFException e) {
            return;
        }
    }
    
    private boolean load32(int[] array, int index) {
        try {
            array[index] = in.readInt();
            return true;
        } catch (EOFException e) {
            return false;
        } catch (IOException e) {
            return false;
        }
    }
    
    public long decodeUnsignedLong() throws IOException {
          long lobits = decodeUnsigned() & 0xffffffffL;
          long hibits = decodeUnsigned() & 0xffffffffL;
          return (hibits<<32)|lobits;
        }
    
    public long decode64bits() throws IOException {
        long lobits = decode32bits() & 0xffffffffL;
        long hibits = decode32bits() & 0xffffffffL;
        return (hibits << 32) | lobits;
    }

    public int decode32bits() throws IOException {
        if (numBitsInReg == 0)
            return in.readInt();
        if (32 > numBitsInReg)
            load();
        int val = (int)(reg & 0xffffffffL);
        reg >>>= 32;
        numBitsInReg -= 32;
        return val;
    }
    
    public double decodeDouble() throws IOException {
        long lobits = decode32bits() & 0xffffffffL;
        long hibits = decode32bits() & 0xffffffffL;
        long bits = (hibits << 32) | lobits;
        return Double.longBitsToDouble(bits);
    }

    public void realign()
    {
        if (numBitsInReg < 32) {
            reg = 0;
            numBitsInReg = 0;
        } else {
            reg >>= (numBitsInReg - 32);
            numBitsInReg = 32;
        }
    }
    
    public void decode(int[] array, int i, int count) throws IOException {
        if (count <= 4) {
            for (; i < count; i++) {
                array[i] = decode32bits();
            }
        } else {
            realign();
            if (numBitsInReg==32) {
                array[i++] = (int)reg;
                reg = 0;
                numBitsInReg = 0;
            }
            for (; i<count; ++i) {
                if (load32(array, i)) break;
            }
        }
    }

    public int decodeUnary() throws IOException {
        if (numBitsInReg < 16)
            load();
        int e, n, v;
        if ((n = (e = unary1[((int)reg & 0xff)]&0xff) + 1) <= 8)
            v = e;
        else if ((n = (e = unary2[((int)reg & 0xffff)]&0xff) + 1) <= 16)
            v = e;
        else
            return _decodeUnary();
        reg >>>= n;
        numBitsInReg -= n;
        return v;
    }

    int _decodeUnary() throws IOException {
        for (int i = 1;; ++i) {
            if (i > numBitsInReg)
                load();
            long msk = (1L << i) - 1;
            if ((reg & msk) == (msk >>> 1)) {
                reg >>>= i;
                numBitsInReg -= i;
                return i - 1;
            }
        }
    }

    public int decodeUnsigned() throws IOException {
        if (numBitsInReg < 16)
            load();
        int e, n, v;
        if ((n = (e = unsigned1[(int)(reg & 0x3fL)]) >>> 5) <= 6)
            v = e & 0x1f;
        else if ((n = (e = unsigned2[(int)(reg & 0x7ffL)]) >>> 12) <= 11)
            v = e & 0xfff;
        else if ((n = (e = unsigned3[(int)(reg & 0xffffL)]) >>> 16) <= 16)
            v = e & 0xffff;
        else
            return _decodeUnsigned();
        reg >>>= n;
        numBitsInReg -= n;
        return v;
    }

    int _decodeUnsigned() throws IOException {
        int nbits = decodeUnary() * 4;
        if (nbits > numBitsInReg)
            load();
        long val = reg & ((nbits < 64) ? ((1L << nbits) - 1) : -1L);
        for (int i = 0; i < nbits; i += 4)
            val += (1L << i);
        reg >>>= nbits;
        numBitsInReg -= nbits;
        return (int)(val & 0xffffffffL);
    }

    public int decodeUnsigned(int n) throws IOException {
        if (n > numBitsInReg)
            load();
        long v = reg & ((n < 64) ? ((1L << n) - 1) : -1L);
        reg >>>= n;
        numBitsInReg -= n;
        return (int)(v & 0xffffffffL);
    }

    private static final byte[] unary1Make() {
        byte table[] = new byte[256];
        for (int i = 0; i < 256; i++) {
            table[i] = (byte)0xff;
            for (int j = 0; j < 8; j++) {
                if ((i & ((1 << (j + 1)) - 1)) != ((1 << j) - 1))
                    continue;
                table[i] = (byte)j;
                break;
            }
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("unary1 "); 
            for (int i=0; i<256; i++) {
                LOG.trace(String.format("%02x \n", table[i])); 
            }
        }
        
        return table;
    }

    static final byte unary1[] = unary1Make();

    private static final byte[] unary2Make() {
        byte table[] = new byte[65536];
        for (int i = 0; i < 65536; i++) {
            table[i] = (byte)0xff;
            for (int j = 0; j < 16; j++) {
                if ((i & ((1 << (j + 1)) - 1)) != ((1 << j) - 1))
                    continue;
                table[i] = (byte)j;
                break;
            }
        }
        return table;
    }

    static final byte unary2[] = unary2Make();

    private static final byte[] unsigned1Make() {
        byte table[] = new byte[64];
        for (int i = 0; i < 64; i++) {
            int entry = unary1[i];
            int bbits = entry * 4;
            int ubits = entry + 1;
            if (bbits + ubits > 6)
                table[i] = (byte)0xe0;
            else {
                int val = (i >>> ubits) & ((1 << bbits) - 1);
                for (int j = 0; j < bbits; j += 4)
                    val += (1 << j);
                table[i] = (byte)(((bbits + ubits) << 5) | val);
            }
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("unsigned1 "); 
            for (int i=0; i<64; i++) {
                LOG.trace(String.format("%02x \n", table[i])); 
            }
        }
        return table;
    }

    static final byte unsigned1[] = unsigned1Make();

    private static final short[] unsigned2Make() {
        short table[] = new short[2048];
        for (int i = 0; i < 2048; i++) {
            int entry = unary2[i];
            int bbits = entry * 4;
            int ubits = entry + 1;
            if (bbits + ubits > 11)
                table[i] = (short)0xf000;
            else {
                int val = (i >>> ubits) & ((1 << bbits) - 1);
                for (int j = 0; j < bbits; j += 4)
                    val += (1 << j);
                table[i] = (short)(((bbits + ubits) << 12) | val);
            }
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("unsigned2 "); 
            for (int i=0; i<64; i++) {
                LOG.trace(String.format("%04x \n", table[i])); 
            }
        }
        return table;
    }

    static final short unsigned2[] = unsigned2Make();

    private static final int[] unsigned3Make() {
        int table[] = new int[65536];
        for (int i = 0; i < 65536; i++) {
            int entry = unary2[i]&0xff;
            int bbits = entry * 4;
            int ubits = entry + 1;
            if (bbits + ubits > 16)
                table[i] = 0xffff0000;
            else {
                int val = (i >>> ubits) & ((1 << bbits) - 1);
                for (int j = 0; j < bbits; j += 4)
                    val += (1 << j);
                table[i] = (((bbits + ubits) << 16) | val);
            }
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("unsigned3 "); 
            for (int i=0; i<64; i++) {
                LOG.trace(String.format("%08x \n", table[i])); 
            }
        }
        return table;
    }

    static final int unsigned3[] = unsigned3Make();
}
