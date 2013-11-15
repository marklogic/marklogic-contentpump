package com.marklogic.mapreduce.test;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.modeler.util.DomUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Node;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSOutput;
import org.w3c.dom.ls.LSSerializer;

import com.marklogic.io.BiendianDataInputStream;
import com.marklogic.mapreduce.ForestInputFormat;
import com.marklogic.tree.CompressedTreeDecoder;
import com.marklogic.tree.ExpandedTree;
import com.marklogic.tree.NodeKind;

// TODO: More rigorous file checks to match stat() in C++ version?
// TODO: Test range, platform byte ordering, bitness.  Auto-detect?
// TODO: buffers larger than int indexes?
// TODO: less long more int; review ranges carefully; can't we assume there won't be more than 2B fragments in a forest?
// TODO: review for proper application of unsigned comparators
// TODO: tune buffer sizes
// TODO: pass files into check functions rather than directory
// TODO: test cases for all fail conditions
// TODO: complete debug trace implementation (in fcheck.cpp too)
// TODO: good command-line parser to support more options flexibility
// TODO: switch back to dedicated endian-ness stream classes?
// TODO: fdatw test is wrong i think

public class FCheck {
    public static final Log LOG = LogFactory.getLog(FCheck.class);
	private static final long maxWrd64 = ((128 << 20) + (16 << 10));
	private static final int CHECKSUM_SEED = 2038074743;
	private static final int CHECKSUM_STEP = 17;

	private boolean verbose = true;
	private boolean debug = true;

	private int wordSize = 4;

	private long numFragments;
	private long numLists;
	private long listDataSize;
	private long treeDataSize;

	private boolean littleEndian = true;

	public FCheck(boolean verbose) {
		this.verbose = verbose;
	}

	private int compareUnsignedLong(long x, long y) {
		return (x == y) ? 0 : ((x < y) ^ ((x < 0) != (y < 0)) ? -1 : 1);
	}

	private void panic(File arg, String msg) {
		panic(arg.getAbsolutePath(), msg);
	}

	private void panic(String arg, String msg) {
		throw new RuntimeException(arg + " " + msg);
	}

	private void panic(String arg1, String arg2, String msg) {
		throw new RuntimeException(arg1 + " " + arg2 + " " + msg);
	}

	private void checkLabel(File dir) {
		File file = new File(dir, "Label");
		if (!file.canRead()) {
			panic(file, "stat");
		}
	}

	private BiendianDataInputStream openFile(File file, int bufferSize)
			throws IOException {
		FileInputStream fis = new FileInputStream(file);
		BufferedInputStream bis = new BufferedInputStream(fis, bufferSize);
		BiendianDataInputStream bdis = new BiendianDataInputStream(bis);
		bdis.setLittleEndian(littleEndian);
		return bdis;
	}

	public void checkForestLabel(File dir) {
		if (verbose)
			System.out.println(dir.getAbsolutePath() + " -> checkForestLabel");
		checkLabel(dir);
		if (verbose)
			System.out.println(dir.getAbsolutePath() + " <- checkForestLabel");
	}

	public void checkStandLabel(File dir) {
		if (verbose)
			System.out.println(dir.getAbsolutePath() + " -> checkStandLabel");
		checkLabel(dir);
		if (verbose)
			System.out.println(dir.getAbsolutePath() + " <- checkStandLabel");
	}

	private boolean isObsolete(File dir) {
		return new File(dir, "Obsolete").exists();
	}

	public void checkFrequencies(File dir) throws IOException {
		File file = new File(dir, "Frequencies");
		if (verbose)
			System.out.println(file.getAbsolutePath() + " -> checkFrequencies");
		BiendianDataInputStream in = openFile(file, 1 << 18);
		long prevKey = -1;
		int position = 0;
		long key;
		long frequency;
		for (;; ++position) {
			try {
				key = in.readLong();
				frequency = in.readLong();
			} catch (EOFException e) {
				break;
			}
			if (frequency > numFragments) {
				panic(file, "frequency out of range" + ", position=" + position
						+ ", frequency=" + frequency + ", numFragments="
						+ numFragments);
			}
			if (prevKey != -1L && compareUnsignedLong(key, prevKey) <= 0) {
				panic(file, "key out of order" + ", position=" + position
						+ ", key=0x" + String.format("%16x", key)
						+ ", prevKey=0x" + String.format("%16x", prevKey));
			}
			prevKey = key;
		}
		if (verbose)
			System.out.println(file.getAbsolutePath()
					+ " <- checkFrequencies [" + position + "]");
	}

	public void checkLinkKeys(File dir) throws IOException {
		File file = new File(dir, "LinkKeys");
		if (verbose)
			System.out.println(file.getAbsolutePath() + " -> checkLinkKeys");
		BiendianDataInputStream in = openFile(file, 1 << 18);
		int position = 0;
		long key;
		try {
			for (;; ++position) {
				key = in.readLong();
				if (key == 0)
					break;
			}
		} catch (EOFException e) {
		}
		if (numFragments == 0)
			numFragments = position;
		else if (position != numFragments) {
			panic(file, "bad count" + ", count=" + position + ", numFragments="
					+ numFragments);
		}
		if (verbose)
			System.out.println(file.getAbsolutePath() + " <- checkLinkKeys ["
					+ position + "]");
	}

	private int computeListChecksum(long key, DataInput in, int words)
			throws IOException {
		// System.out.println(String.format("key %08x", key));
		int cksum = CHECKSUM_SEED;
		// System.out.println(String.format("seed %08x", cksum));
		cksum = (cksum + (int) (key >> 32)) * CHECKSUM_STEP;
		// System.out.println(String.format("start 1 %08x", cksum));
		cksum = (cksum + (int) (key)) * CHECKSUM_STEP;
		// System.out.println(String.format("start 2 %08x", cksum));
		while (0 < words--) {
			cksum = (cksum + in.readInt()) * CHECKSUM_STEP;
			// System.out.println(String.format("step %08x", cksum));
		}
		// System.out.println(String.format("final %08x", (cksum &
		// 0xfffffff0)));
		return cksum & 0xfffffff0;
	}

	public void checkListData(File dir) throws IOException {
		File file = new File(dir, "ListData");
		if (verbose)
			System.out.println(file.getAbsolutePath() + " -> checkListData");
		listDataSize = file.length();
		BiendianDataInputStream in = openFile(file, 1 << 18);
		long prevKey = -1;
		int position = 0;
		long key;
		int csword, checksum, datWords, hdrWords, fdatw;
		for (;; ++position) {
			try {
				key = ((long) in.readInt()) << 32
						| (((long) in.readInt()) & 0xffffffffL);
				csword = in.readInt();
				checksum = csword & 0xfffffff0;
				datWords = csword & 0x0000000f;
				hdrWords = 3;
				if (datWords == 0) {
					datWords = in.readInt();
					hdrWords = 4;
				}
				in.getInputStream().mark(4);
				fdatw = in.readInt();
				in.getInputStream().reset();
			} catch (EOFException e) {
				break;
			}
			if (key == -1L && csword == -1 && fdatw == -1)
				continue;
			if (prevKey != -1 && compareUnsignedLong(key, prevKey) <= 0) {
				panic(file,
						String.format(
								"key out of order, position=%d, key=0x%016x, prevKey=0x%016x",
								position, key, prevKey));
			}
			prevKey = key;
			if (datWords < 1 || datWords > maxWrd64 - 4) {
				panic(file,
						String.format(
								"bad word count, position=%d, key=0x%16x, hdrWords=%d, datWords=%d, checksum=0x%08x",
								position, key, hdrWords, datWords, checksum));
			}
			int computed = computeListChecksum(key, in, datWords);
			if (checksum != computed) {
				panic(file,
						String.format(
								"bad checksum, position=%d, key=0x%016x, hdrWords=%d, datWords=%d, checksum=0x%08x, computed=0x%08x",
								position, key, hdrWords, datWords, checksum,
								computed));
			}
		}
		numLists = position;
		if (verbose)
			System.out.println(file.getAbsolutePath() + " <- checkListData ["
					+ position + "]");
	}

	public void checkListIndex(File dir) throws IOException {
		File file = new File(dir, "ListIndex");
		if (verbose)
			System.out.println(file.getAbsolutePath() + " -> checkListIndex");
		BiendianDataInputStream in = openFile(file, 1 << 18);
		long prevKey = -1L;
		long prevOffset = -1L;
		long key, offset;
		int position = 0;
		for (;; ++position) {
			try {
				key = in.readLong();
				offset = in.readLong();
			} catch (EOFException e) {
				break;
			}
			if (compareUnsignedLong(position, numLists) >= 0) {
				panic(file,
						String.format(
								"position out of range, position=%d, key=0x%016x, numFragments=%d",
								position, key, numFragments));
			}
			if (prevKey != -1L && compareUnsignedLong(key, prevKey) <= 0) {
				panic(file,
						String.format(
								"key out of order, position=%d, key=0x%016x, prevKey=0x%016x",
								position, key, prevKey));
			}
			prevKey = key;
			if (prevOffset != -1L
					&& compareUnsignedLong(offset, prevOffset) <= 0) {
				panic(file,
						String.format(
								"offset out of order, position=%d, offset=%d, prevOffset=%d",
								position, offset, prevOffset));
			}
			prevOffset = offset;
		}
		if (verbose)
			System.out.println(file.getAbsolutePath() + " <- checkListIndex ["
					+ position + "]");
	}

	public void checkOrdinals(File dir) throws IOException {
		File file = new File(dir, "Ordinals");
		if (verbose)
			System.out.println(file.getAbsolutePath() + " -> checkOrdinals");
		BiendianDataInputStream in = openFile(file, 1 << 18);
		int position = 0;
		@SuppressWarnings("unused")
		long ordinal;
		try {
			for (;; ++position) {
				ordinal = in.readLong();
			}
		} catch (EOFException e) {
		}
		if (compareUnsignedLong((long) position, numFragments) < 0) {
			panic(file, String.format("bad count, count=%d, numFragments=%d",
					position, numFragments));
		}
		if (verbose)
			System.out.println(file.getAbsolutePath() + " <- checkOrdinals ["
					+ position + "]");
	}

	public void checkQualities(File dir) throws IOException {
		File file = new File(dir, "Qualities");
		if (verbose)
			System.out.println(file.getAbsolutePath() + " -> checkQualities");
		BiendianDataInputStream in = openFile(file, 1 << 18);
		int position = 0;
		@SuppressWarnings("unused")
		int quality;
		try {
			for (;; ++position) {
				quality = in.readInt();
			}
		} catch (EOFException e) {
		}
		if ((long)position < numFragments) {
			panic(file, String.format("bad count, count=%d, numFragments=%d",
					position, numFragments));
		}
		if (verbose)
			System.out.println(file.getAbsolutePath() + " <- checkQualities ["
					+ position + "]");
	}

	public void checkStopKeySet(File dir) throws IOException {
		File file = new File(dir, "StopKeySet");
		if (verbose)
			System.out.println(file.getAbsolutePath() + " -> checkStopKeySet");
		BiendianDataInputStream in = openFile(file, 1 << 18);
		long prevKey = -1;
		int position = 0;
		long key;
		try {
			for (;; ++position) {
				key = in.readLong();
				if (key == 0)
					break;
				if (prevKey != -1 && key <= prevKey) {
					panic(file,
							String.format(
									"key out of order, position=%d, key=0x%016x, prevKey=0x%016x",
									position, key, prevKey));
				}
				prevKey = key;
			}
		} catch (EOFException e) {
		}
		if (verbose)
			System.out.println(file.getAbsolutePath() + " <- checkStopKeySet ["
					+ position + "]");
	}

	public void checkTimestamps(File dir) throws IOException {
		File file = new File(dir, "Timestamps");
		if (verbose)
			System.out.println(file.getAbsolutePath() + " -> checkTimestamps");
		BiendianDataInputStream in = openFile(file, 1 << 18);
		int position = 0;
		long nascent;
		long deleted;

		try {
			for (;; ++position) {
				nascent = in.readLong();
				deleted = in.readLong();
				if (nascent == 0L && deleted == 0L)
					break;
				if (compareUnsignedLong(deleted, nascent) < 0 && nascent != -1) {
					panic(file,
							String.format(
									"bad timestamp, position=%d, nascent=%d, deleted=%d",
									position, nascent, deleted));
				}
			}
		} catch (EOFException e) {
		}
		if (numFragments == 0L)
			numFragments = position;
		else if (position != numFragments) {
			panic(file, String.format("bad count, count=%d, numFragments=%d",
					position, numFragments));
		}
		if (verbose)
			System.out.println(file.getAbsolutePath() + " <- checkTimestamps ["
					+ position + "]");
	}

	private int computeTreeChecksum(int key, DataInput in, int words)
			throws IOException {
		// System.out.println(String.format("key:%08x", key));
		int cksum = CHECKSUM_SEED;
		// System.out.println(String.format("seed:%08x", cksum));
		cksum = ((cksum + key) * CHECKSUM_STEP) & 0xffffffff;
		// System.out.println(String.format("start:%08x", cksum));
		while (0 < words--) {
			int w = in.readInt();
			cksum = ((cksum + w) * CHECKSUM_STEP) & 0xffffffff;
			// System.out.println(String.format("%08x : %08x", cksum, w));
		}
		return cksum & 0xfffffff0;
	}

	public void checkTreeData(File dir) throws IOException {
		File file = new File(dir, "TreeData");
		if (verbose)
			System.out.println(file.getAbsolutePath() + " -> checkTreeData");
		treeDataSize = file.length();
		BiendianDataInputStream in = openFile(file, 1 << 18);
		int prevDocid = -1;
		int position = 0;
		int docid, csword, checksum, datWords, hdrWords, fdatw;
		for (;; ++position) {
			try {
				docid = in.readInt();
				csword = in.readInt();
				checksum = csword & 0xfffffff0;
				datWords = csword & 0x0000000f;
				hdrWords = 2;
				if (datWords == 0) {
					datWords = in.readInt();
					hdrWords = 3;
				}
				in.getInputStream().mark(4);
				fdatw = in.readInt();
				in.getInputStream().reset();
			} catch (EOFException e) {
				break;
			}
			if (docid == 0xffffffff && csword == 0xffffffff
					&& fdatw == 0xffffffff) {
				continue;
			}
			if (debug) {
				System.out.println(String.format(
						"TreeData p %08x d %08x c %016x", position, docid,
						checksum));
			}
			if (prevDocid != -1 && (long) docid <= (long) prevDocid) {
				panic(file, "docid out of order" + ", position=" + position
						+ ", docid=" + docid + ", prevDocid=" + prevDocid);
			}
			prevDocid = docid;
			if (datWords < 1 || datWords > maxWrd64 - 4) {
				panic(file,
						"bad word count" + ", position=" + position
								+ ", docid=" + docid + ", hdrWords=" + hdrWords
								+ ", datWords=" + datWords + ", checksum=0x"
								+ Integer.toHexString(checksum));
			}
			int computed = computeTreeChecksum(docid, in, datWords);
			if (checksum != computed) {
				panic(file,
						"bad checksum" + ", position=" + position + ", docid="
								+ docid + ", hdrWords=" + hdrWords
								+ ", datWords=" + datWords + ", checksum=0x"
								+ Integer.toHexString(checksum)
								+ ", computed=0x"
								+ Integer.toHexString(computed));
			}
		}
		if (verbose)
			System.out.println(file.getAbsolutePath() + " <- checkTreeData ["
					+ position + "]");
	}

	public void decodeTreeData(File dir) throws IOException {
        File file = new File(dir, "TreeData");
//        LSSerializer writer;
//        LSOutput lsout;
//        try {
////        	DOMImplementationRegistry registry = DOMImplementationRegistry.newInstance();
////        	DOMImplementationLS impl = (DOMImplementationLS)registry.getDOMImplementation("XML 3.0");
////        	writer = impl.createLSSerializer();
//        	//writer.getDomConfig().setParameter("canonical-form", "true");
//        	lsout = impl.createLSOutput();
//        	lsout.setCharacterStream(new PrintWriter(System.out));
//        	lsout.setEncoding("UTF-8");
//        }
//        catch (Exception e) {
//        	throw new RuntimeException("Unable to initialize XML serialization.", e);
//        }
        if (verbose)
            System.out.println(file.getAbsolutePath() + " -> checkTreeData");
        treeDataSize = file.length();
        BiendianDataInputStream in = openFile(file, 1 << 18);
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
                if (docid == 0xffffffff && csword == 0xffffffff && fdatw == 0xffffffff) {
                    System.out.println("Reached the end.");
                    break;
                }
                if (prevDocid != -1 && docid <= prevDocid) {
                    panic(file, "docid out of order, position=" + position +
                          ", docid=" + docid + ", prevDocid=" + prevDocid);
                }
                prevDocid = docid;
                if (hdrWords == 2) {
                    j = datWords-1;
                } else {
                    j = datWords;
                }
                j *= 4;
            } catch (EOFException e) {
                break;
            }
            
            if (debug) {
                System.out.println(String.format("\n\nTreeData p %d d %d c %016x", position, docid, checksum));
            }
            
            System.out.println("POSITION " + position);
            System.out.println("docid=" + docid + " datWords=" + datWords);
            
            try {
//                in.setLittleEndian(false);
            	in.getInputStream().mark(j);
            	ExpandedTree tree = new CompressedTreeDecoder().decode(in);
            	// TODO: count and verify bytes read
//            	int computed = computeChecksum(docid, in, datWords);
                System.out.println(tree.getDocumentURI());
                byte kind = tree.rootNodeKind();
                if (kind == NodeKind.BINARY) {
                    System.out.println("binary root");
                } else if (kind == NodeKind.ELEM) {
                    System.out.println("element root");
                } else if (kind == NodeKind.TEXT) {
                    System.out.println("text root");
                } else {
                    System.out.println("unexpected node kind: " + kind);
                }
//                ByteArrayOutputStream bos = new ByteArrayOutputStream();
//                DomUtil.writeXml(tree.rootNode(), bos);
//                System.out.println(bos.toString());
            }
            catch (Exception e) {
            	System.err.println("Fail at position " + position);
            	e.printStackTrace();
            }
            
        	in.getInputStream().reset();
            while (j > 0) {
                long actual = in.getInputStream().skip(j);
                if (actual < j) {
                    j -= actual;
                } else if (actual > j){
                    panic(file, "Over-skipped: actual=" + actual + ",j=" + j);
                } else {
                    break;
                }
            }
        }
        if (verbose)
            System.out.println(file.getAbsolutePath() + " <- checkTreeData [" + position + "]");
    }

	public void checkTreeIndex(File dir) throws IOException {
		File file = new File(dir, "TreeIndex");
		if (verbose)
			System.out.println(file.getAbsolutePath() + " -> checkTreeIndex");
//		BiendianDataInputStream in = openFile(file, 1 << 18);
		Path path = new Path(dir.getAbsolutePath());
		FileSystem fs = path.getFileSystem(new Configuration());
		FileStatus children[] = fs.listStatus(path);
        FileStatus treeIndexStatus = null, treeDataStatus = null;
        for (FileStatus child : children) {
            String fileName = child.getPath().getName();
            if (fileName.equals("TreeData")) { // inside a stand
                treeDataStatus = child;
            } else if (fileName.equals("TreeIndex")) {
                treeIndexStatus = child;
            }
            if (treeDataStatus != null && treeIndexStatus != null) {
                break;
            }
        }
        if (treeDataStatus == null) {
            throw new RuntimeException("TreeData file not found.");
        } else if (treeIndexStatus == null) {
            throw new RuntimeException("TreeIndex file not found.");
        }
        long treeDataSize = treeDataStatus.getLen();
        if (treeDataSize == 0) {
            // unexpected, give up this stand
            LOG.warn("Found empty TreeData file.  Skipping...");
            return;
        }
        FSDataInputStream is = fs.open(treeIndexStatus.getPath());
        BiendianDataInputStream in = new BiendianDataInputStream(is);
        in.setLittleEndian(littleEndian);
		int prevDocid = -1;
		long prevOffset = -1L;
		int position = 0;
		int docid;
		long offset;
		for (;; ++position) {
			try {
				docid = in.readInt();
				in.readInt();
				offset = in.readLong();				
			} catch (EOFException e) {
				break;
			}
			if (debug) {
				System.out.println(String.format(
						"TreeIndex p %08x d %08x o %016x", position, docid,
						offset));
			}
			if (compareUnsignedLong(offset, treeDataSize) >= 0) {
				panic(file,
						String.format(
								"offset out of range, position=%d, offset=%d, treeDataSize=%d",
								position, offset, treeDataSize));
			}
			if (prevDocid != -1
					&& (docid & 0xffffffffL) <= (prevDocid & 0xffffffffL)) {
				panic(file,
						String.format(
								"docid out of order, position=%d, docid=%d, prevDocid=%d",
								position, docid, prevDocid));
			}
			prevDocid = docid;
			if (prevOffset != -1L
					&& compareUnsignedLong(offset, prevOffset) <= 0) {
				panic(file,
						String.format(
								"offset out of order, position=%d, offset=%d, prevOffset=%d",
								position, offset, prevOffset));
			}
			prevOffset = offset;
		}
		if (verbose)
			System.out.println(file.getAbsolutePath() + " <- checkTreeIndex ["
					+ position + "]");
	}

	public void checkUniqKeys(File dir) throws IOException {
		File file = new File(dir, "UniqKeys");
		if (verbose)
			System.out.println(file.getAbsolutePath() + " -> checkUniqKeys");
		BiendianDataInputStream in = openFile(file, 1 << 18);
		int position = 0;
		long key;
		for (;; ++position) {
			try {
				key = in.readLong();
				if (key == 0)
					break;
			} catch (EOFException e) {
				break;
			}
		}

		if (numFragments == 0L)
			numFragments = position;
		if ((long) position != numFragments) {
			panic(file, String.format("bad count, count=%d, numFragments=%d",
					position, numFragments));
		}

		if (verbose)
			System.out.println(file.getAbsolutePath() + " <- checkUniqKeys ["
					+ position + "]");
	}

	public void checkURIKeys(File dir) throws IOException {
		File file = new File(dir, "URIKeys");
		if (verbose)
			System.out.println(file.getAbsolutePath() + " -> checkURIKeys");
		BiendianDataInputStream in = openFile(file, 1 << 18);
		int position = 0;
		long key;
		for (;; ++position) {
			try {
				key = in.readLong();
				if (key == 0)
					break;
			} catch (EOFException e) {
				break;
			}
		}

		if (numFragments == 0L)
			numFragments = position;
		if ((long) position != numFragments) {
			panic(file, String.format("bad count, count=%d, numFragments=%d",
					position, numFragments));
		}

		if (verbose)
			System.out.println(file.getAbsolutePath() + " <- checkURIKeys ["
					+ position + "]");
	}

	public void checkRangeIndexes(File dir) {
	}

	public void checkStand(File dir) throws IOException {
		if (verbose)
			System.out.println(dir.getAbsolutePath() + " -> checkStand");
		if (isObsolete(dir))
			return;
		numFragments = 0;
		checkStandLabel(dir);
		checkListData(dir);
		checkLinkKeys(dir);
		checkFrequencies(dir);
		checkListIndex(dir);
		checkOrdinals(dir);
		checkQualities(dir);
		checkStopKeySet(dir);
		checkTimestamps(dir);
		checkTreeData(dir);
		checkTreeIndex(dir);
		checkUniqKeys(dir);
		checkURIKeys(dir);
		checkRangeIndexes(dir);
		decodeTreeData(dir);
		if (verbose)
			System.out.println(dir.getAbsolutePath() + " <- checkStand");
	}

	public void checkJournals(File dir) {
		// if (verbose) System.out.println(dir.getAbsolutePath() +
		// " -> checkJournals");
		// if (verbose) System.out.println(dir.getAbsolutePath() +
		// " <- checkJournals");
	}

	public void checkForest(File dir) throws IOException {
		if (verbose)
			System.out.println(dir.getAbsolutePath() + " -> checkForest ("
					+ (littleEndian ? "Little" : "Big") + " Endian)");
		checkForestLabel(dir);
		File files[] = dir.listFiles();
		Arrays.sort(files);
		for (File file : files) {
			String filename = file.getName();
			if (file.isDirectory()) {
				if (filename.equals("Journals")) {
					checkJournals(file);
				} else if (!filename.equals("Large")){
					checkStand(file);
				}
			} else {
				if (!filename.equals("Label") && !filename.equals("Label_1")) {
					panic(file, "unexpected");
				}
			}
		}
		if (verbose)
			System.out.println(dir.getAbsolutePath() + " <- checkForest");
	}

	public void fcheck(File dir) throws IOException {
		if (verbose)
			System.out.println(dir.getAbsolutePath() + " -> fcheck ");
		littleEndian = !"Solaris".equals(System.getProperty("os.name"));
		try {
			try {
				checkForest(dir);
			} catch (Exception e) {
				if ((e.getMessage() != null)
						&& e.getMessage().indexOf("bad checksum") >= 0) {
					if (verbose)
						System.out.println(e.getMessage());
					littleEndian = !littleEndian;
					checkForest(dir);
				} else {
					throw e;
				}
			}
			if (verbose)
				System.out.println(dir.getAbsolutePath() + " <- fcheck OK");
		} catch (Exception e) {
	        e.printStackTrace();
	        System.out.println(dir.getAbsolutePath() + " <- fcheck FAIL");
		}
	}

	public static void main(String[] argv) throws IOException {
		if ((argv.length < 1) || (argv.length > 2)
				|| ((argv.length == 2) && !argv[0].equals("-v"))) {
			System.err.println("usage: " + FCheck.class.getName()
					+ " [-v] forestpath");
			System.exit(1);
		}
		new FCheck(argv.length > 1).fcheck(new File(argv[argv.length - 1]));
		System.exit(0);
	}
}
