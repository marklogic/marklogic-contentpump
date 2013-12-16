package com.marklogic;

import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.marklogic.io.BiendianDataInputStream;
import com.marklogic.tree.CompressedTreeDecoder;
import com.marklogic.tree.ExpandedTree;

public class Utils {
    public static List<ExpandedTree> decodeTreeData(File dir, boolean verbose)
        throws IOException {
        LinkedList<ExpandedTree> treeList = new LinkedList<ExpandedTree>();
        File file = new File(dir, "TreeData");
        if (verbose)
            System.out.println(file.getAbsolutePath() + " -> checkTreeData");
        // long treeDataSize = file.length();
        BiendianDataInputStream in = openFile(file, 1 << 18, true);
        int position = 0;
        int docid, csword, checksum, datWords, fdatw, hdrWords = 2, j;
        long prevDocid = -1;
        for (;; ++position) {
            try {
                docid = in.readInt();
                csword = in.readInt();
                fdatw = in.readInt();
                checksum = csword & 0xfffffff0;
                datWords = csword & 0x0000000f;
                if (datWords == 0) {
                    datWords = fdatw;
                    hdrWords = 3;
                    System.out.println("3 header words");
                }
                if (docid == 0xffffffff && csword == 0xffffffff
                    && fdatw == 0xffffffff) {
                    System.out.println("Reached the end.");
                    break;
                }
                if (prevDocid != -1 && docid <= prevDocid) {
                    panic(file, "docid out of order, position=" + position
                        + ", docid=" + docid + ", prevDocid=" + prevDocid);
                }
                prevDocid = docid;
                if (hdrWords == 2) {
                    j = datWords - 1;
                } else {
                    j = datWords;
                }
                j *= 4;
            } catch (EOFException e) {
                break;
            }

            // if (debug) {
            // System.out.println(String.format("\n\nTreeData p %d d %d c %016x",
            // position, docid, checksum));
            // }
            if (verbose) {
                System.out.println("POSITION " + position);
                System.out.println("docid=" + docid + " datWords=" + datWords);
            }
            try {
                // in.setLittleEndian(false);
                in.getInputStream().mark(j);
                ExpandedTree tree = new CompressedTreeDecoder().decode(in);
                treeList.add(tree);
            } catch (Exception e) {
                System.err.println("Fail at position " + position);
                e.printStackTrace();
            }
            in.getInputStream().reset();
            while (j > 0) {
                long actual = in.getInputStream().skip(j);
                if (actual < j) {
                    j -= actual;
                } else if (actual > j) {
                    panic(file, "Over-skipped: actual=" + actual + ",j=" + j);
                } else {
                    break;
                }
            }
        }
        return treeList;
    }

    public static BiendianDataInputStream openFile(File file, int bufferSize,
        boolean littleEndian) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        BufferedInputStream bis = new BufferedInputStream(fis, bufferSize);
        BiendianDataInputStream bdis = new BiendianDataInputStream(bis);
        bdis.setLittleEndian(littleEndian);
        return bdis;
    }

    private static void panic(File arg, String msg) {
        panic(arg.getAbsolutePath(), msg);
    }

    private static void panic(String arg, String msg) {
        throw new RuntimeException(arg + " " + msg);
    }

    public static Document readXMLasDOMDocument(File file) {
        Document doc = null;
        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory
                .newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            doc = dBuilder.parse(file);

            // optional, but recommended
            // read this -
            // http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
            doc.getDocumentElement().normalize();

 
        } catch (Exception e) {
            e.printStackTrace();
        }
        return doc;
    }

}
