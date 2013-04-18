/*
 * Copyright 2003-2013 MarkLogic Corporation
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
package com.marklogic.mapreduce.utilities;

import java.util.LinkedHashSet;
import java.util.PriorityQueue;

import com.marklogic.mapreduce.DocumentURI;

public class StatisticalAssignmentPolicy extends AssignmentPolicy {
    //stats[NUM_FORESTS];
//    private long []stats;
    
    //since we need to synchronize both pq and docCount,
    //we manually synchronize pq instead of using PriorityBlockingQueue
    private PriorityQueue<Stats> pq;
    private long [] docCount;
    /**
     * forests ( RO/DO, retired forests are excluded)
     * can we reuse the forest array in ContentWriter?
     */
    private String[] forests;
    
    public StatisticalAssignmentPolicy (long [] stats, LinkedHashSet<String> uForests) {
        this.forests = uForests.toArray(new String[uForests.size()]);
        policy = AssignmentPolicy.Kind.STATISTICAL;
        docCount = new long[stats.length];
        pq = new PriorityQueue<Stats>(stats.length);
        
        for (int i=0; i<stats.length; i++) {
            pq.add(new Stats(i, stats[i]));
            docCount[i] = stats[i];
        }
    }
    
    private int getIdxSmallestForest() {
        Stats min = null;
        synchronized(pq) {
            min = pq.peek();
        }
        //TODO change to fine
        if (LOG.isDebugEnabled()) {
            LOG.debug("picked forest# " + min.getfIdx() + " with "
                + min.getDocCount() + " docs");
        }
        return min.getfIdx();
    }
    
    /**
     * add doc count to forest with index fIdx
     * @param fIdx
     * @param count
     */
    public void updateStats(int fIdx, long count) {
        synchronized (pq) {
            docCount[fIdx] += count; 
            Stats tmp = new Stats(fIdx, docCount[fIdx]);
            //remove the stats object with the same fIdx
            pq.remove(tmp);
            pq.add(tmp);
        }
        //TODO change to fine
        if (LOG.isDebugEnabled()) {
            LOG.debug("update forest " + fIdx);
        }
    }
    
    @Override
    public String getPlacementForestId(DocumentURI uri) {
        // TODO Auto-generated method stub
        // doesn't seem related to uri
        return forests[getPlacementForestIndex(uri)];
    }

    /**
     * get the index of the forest with smallest number of docs
     */
    @Override
    public int getPlacementForestIndex(DocumentURI uri) {
        return getIdxSmallestForest();
    }
    
    private class Stats implements Comparable<Stats>{
        int fIdx;
        long docCount;
        
        public void setfIdx(int fIdx) {
            this.fIdx = fIdx;
        }

        public void setDocCount(long docCount) {
            this.docCount = docCount;
        }

        public int getfIdx() {
            return fIdx;
        }

        public long getDocCount() {
            return docCount;
        }

        public Stats(int fIdx, long docCount) {
            super();
            this.fIdx = fIdx;
            this.docCount = docCount;
        }

        public int compareTo(Stats o) {
            if (docCount > o.docCount) return 1;
            else if (docCount < o.docCount) return -1;
            else return 0;
        }
        
        public boolean equals(Object obj) {
            if (obj == null)
                return false;
            if (obj == this)
                return true;
            if (obj.getClass() != getClass())
                return false;
            return fIdx == ((Stats)obj).getfIdx();
        }
    }

}
