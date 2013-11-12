package com.marklogic.io;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;

public class Decoder {
    
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
        // System.out.print("unary1 "); for (int i=0; i<256; i++) {
        // System.out.print(String.format("%02x ", table[i])); }
        // System.out.println();
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
        // System.out.print("unsigned1 "); for (int i=0; i<64; i++) {
        // System.out.print(String.format("%02x ", table[i])); }
        // System.out.println();
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
        // System.out.print("unsigned2 "); for (int i=0; i<64; i++) {
        // System.out.print(String.format("%04x ", table[i])); } 
        // System.out.println();
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
        /*
        int table[] = new int[65536];
        for (int i = 0; i < 65536; i++) {
            long entry = unary2[i]&0xffL;
            long bbits = entry * 4L;
            long ubits = entry + 1L;
            if (((bbits + ubits)&0xffffL) > 16L)
                table[i] = 0xffff0000;
            else {
            	long val = (i >>> ubits) & ((1 << bbits) - 1);
                for (int j = 0; j < bbits; j += 4)
                    val += (1 << j);
                table[i] = (int)(((bbits + ubits) << 16) | val);
            }
        }
        */
        // System.out.print("unsigned3 "); for (int i=0; i<64; i++) {
        // System.out.print(String.format("%08x ", table[i])); } 
        // System.out.println();
        return table;
    }

    static final int unsigned3[] = unsigned3Make();

/*
    static const unsigned short*
    signed1Make()
    {
      unsigned short* table = new unsigned short[64];
      for (unsigned i=0; i<64; i++) {
        unsigned entry = Decoder::unsigned1[i];
        unsigned nbits = entry>>>5;
        unsigned d = entry&0x1f;
        unsigned value = ((d&1)?(int)((d+1)/2):-(int)(d/2));
        table[i] = (unsigned short)((nbits<<8)|value);
      }
      return table;
    }

    const unsigned short* const Decoder::signed1 = signed1Make();

    static const unsigned*
    signed2Make()
    {
      unsigned* table = new unsigned[2048];
      for (unsigned i=0; i<2048; i++) {
        unsigned entry = Decoder::unsigned2[i];
        unsigned nbits = entry>>>12;
        unsigned d = entry&0xfff;
        unsigned value = ((d&1)?(int)((d+1)/2):-(int)(d/2));
        table[i] = (nbits<<16)|value;
      }
      return table;
    }

    const unsigned* const Decoder::signed2 = signed2Make();

    static const unsigned*
    signed3Make()
    {
      unsigned* table = new unsigned[65536];
      for (unsigned i=0; i<65536; i++) {
        unsigned entry = Decoder::unsigned3[i];
        unsigned nbits = entry>>>16;
        unsigned d = entry&0xffff;
        unsigned value = ((d&1)?(int)((d+1)/2):-(int)(d/2));
        table[i] = (nbits<<16)|value;
      }
      return table;
    }

    const unsigned* const Decoder::signed3 = signed3Make();
*/
}
