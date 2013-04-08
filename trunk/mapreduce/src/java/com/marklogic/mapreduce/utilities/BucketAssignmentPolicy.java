package com.marklogic.mapreduce.utilities;

import java.util.LinkedHashSet;

import com.marklogic.mapreduce.DocumentURI;


public class BucketAssignmentPolicy extends AssignmentPolicy {
    static final int NUM_BUCKET = 1<<14;
    //TODO maybe use int[][] is better than ArrayList<ArrayList>, since bucket table doesn't grow in mlcp
    private int [][] buckets;
    /**
     * forests ( including RO/DO, but retired forests are excluded)
     */
    private String[] forests;
    
    public BucketAssignmentPolicy(String[] forests, LinkedHashSet<String> uForests) {
        buckets = new int[forests.length][NUM_BUCKET];
        initBucketsTable(forests.length);
        this.forests = forests;
        this.uForests = uForests;
        policy = Kind.BUCKET;
    }

    public int[][] getBucketsTable() {
        return buckets;
    }
    
    private void initBucketsTable(int maxSize) {
        // long[] assignment = new long[NUM_BUCKET];
        // long i = 10;
        // long [] a = new long[i];
        for (int i = 2; i < maxSize; i++) {
            // bucket to forest assignment
            int[] assignment = new int [NUM_BUCKET]; 
            int expectCount[] = new int[maxSize];
            int currentCount[] = new int[maxSize];

            for (int j = 0; j < NUM_BUCKET; j++)
                assignment[j] = 0;

            for (int forestCount = 2; forestCount <= maxSize; forestCount++) {

                int minAmount = NUM_BUCKET / forestCount;
                int remainAmount = NUM_BUCKET - (minAmount * forestCount);

                // assignment the number of buckets to each forest first
                for (int k = 0; k < forestCount; k++) {
                    expectCount[k] = minAmount;
                    if (remainAmount > 0) {
                        expectCount[k]++;
                        remainAmount--;
                    }
                    currentCount[k] = 0;
                }

                int newForest = forestCount - 1;
                for (int j = 0; j < NUM_BUCKET; j++) {
                    int forest = assignment[j];

                    // each forest keep the expected number of bucket
                    // and give the rest to the new forest
                    if (currentCount[forest] < expectCount[forest]) {
                        currentCount[forest]++;
                    } else {
                        assignment[j] =  newForest;
                        currentCount[newForest]++;
                    }
                }
            }
            buckets[i] = assignment;
        }
    }
    
    /**
     * return the forest id
     * @param uri
     * @return
     */
    public String getPlacementForestId(DocumentURI uri) {
        int fIdx = getBucketPlacementId(uri, buckets, NUM_BUCKET, forests.length, uForests.size());
        return forests[fIdx];

    }
    
    /**
     * return the index to the list of updatable forests (all forest - retired - RO/DO)
     * @param uri
     * @return
     */
    public int getPlacementForestIndex(DocumentURI uri) {
        return getBucketPlacementId(uri, buckets, NUM_BUCKET, forests.length, uForests.size());
    }
    
    //return the index to the forest list (all forest - retired - RO/DO)
    private int getBucketPlacementId(DocumentURI uri, int [][]buckets, int numBuckets, int numForests, int uForests) {
        LegacyAssignmentPolicy lap = new LegacyAssignmentPolicy();
        int bucket = lap.getPlacementId(uri, numBuckets);
        int fIdx = buckets[numForests-1][bucket];
        boolean allUpdateable = true;
        int [] partv = new int[uForests];
        for (int i = 0; i<numForests; i++) {
            if (!isUpdatable(fIdx)) {
                if (i==fIdx) {
                    allUpdateable = false;
                }
            } else {
                partv[i] = fIdx;
            }
        }
        if (!allUpdateable) {
            fIdx = partv[lap.getPlacementId(uri, uForests)];
        }
        return fIdx;
    }
    
    private boolean isUpdatable(int fIdx) {
        String forestId = forests[fIdx];
        return uForests.contains(forestId);
    }
    
}
