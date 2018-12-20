package com.marklogic.mapreduce.utilities;

import java.math.BigInteger;
import java.util.LinkedHashSet;

import com.marklogic.mapreduce.DocumentURI;

/**
 * Segment Assignment Policy for fastload
 * 
 * @author jchen
 */
public class SegmentAssignmentPolicy extends LegacyAssignmentPolicy {
    public static long rotr(BigInteger value, int shift) {
        return value.shiftRight(shift).xor(value.shiftLeft(64 - shift))
            .longValue();
    }
    
    public SegmentAssignmentPolicy() {
    }

    public SegmentAssignmentPolicy(LinkedHashSet<String> uForests) {
        super(uForests);
        policy = Kind.SEGMENT;
    }
    
    public static int getPlacementId(DocumentURI uri, int size) {
        switch (size) {
        case 0:
            throw new IllegalArgumentException("getPlacementId(size = 0)");
        case 1:
            return 0;
        default:
            String nk = normalize(uri.getUri());
            BigInteger uriKey = getUriKey(nk);
            long u = uriKey.longValue();
            u ^= rotr(uriKey,2);
            u ^= rotr(uriKey,3);
            u ^= rotr(uriKey,5);
            u ^= rotr(uriKey,7);
            u ^= rotr(uriKey,11);
            u ^= rotr(uriKey,13);
            u ^= rotr(uriKey,17);
            u ^= rotr(uriKey,19);
            u ^= rotr(uriKey,23);
            u ^= rotr(uriKey,29);
            u ^= rotr(uriKey,31);
            u ^= rotr(uriKey,37);
            u ^= rotr(uriKey,41);
            u ^= rotr(uriKey,43);
            u ^= rotr(uriKey,47);
            u ^= rotr(uriKey,53);
            u ^= rotr(uriKey,59);
            u ^= rotr(uriKey,61);
            return (int) Long.remainderUnsigned(u,size);
        }
    }
    
    @Override
    public int getPlacementForestIndex(DocumentURI uri) {
        return getPlacementId(uri, forests.length);
    }
}
