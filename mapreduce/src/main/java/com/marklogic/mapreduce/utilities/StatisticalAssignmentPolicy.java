/*
 * Copyright 2003-2016 MarkLogic Corporation
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

import java.util.concurrent.PriorityBlockingQueue;

import com.marklogic.mapreduce.DocumentURI;

/**
 * Statistical Assignment Policy for fastload 
 */
public class StatisticalAssignmentPolicy extends AssignmentPolicy {
    private PriorityBlockingQueue<Stats> pq;
    private long[] frmtCount;
    private int batch;
    public StatisticalAssignmentPolicy(long[] stats, int batch) {
        policy = AssignmentPolicy.Kind.STATISTICAL;
        frmtCount = new long[stats.length];
        this.batch = batch;
        pq = new PriorityBlockingQueue<>(stats.length);

        for (int i = 0; i < stats.length; i++) {
            pq.add(new Stats(i, stats[i]));
            frmtCount[i] = stats[i];
        }
    }

    /**
     * Most of the time, frmtCount increase by batch; instead of waiting the
     * batch actually accumulated, we increment the batch in the priority. In
     * this way, we merge two operations (peek and remove) into one operation
     * (take). What's more, the take operation only takes O(1) time while the
     * remove operation takes O(n) time, where n is # forests.
     * 
     * The down-side is, if insertContentCollectErrors happens to collect
     * errors, # of docs with errors is not excluded in the stats in the
     * queue
     * 
     * @return forest index
     * @throws InterruptedException
     */
    private int popAndInsert() throws InterruptedException {
        //take() will wait if pq is temporarily empty
        Stats min = pq.take();
        if (LOG.isTraceEnabled()) {
            LOG.trace("picked forest# " + min.fIdx + " with "
                + min.frmtCount + " docs");
        }
        min.frmtCount += batch;
        pq.offer(min);
        int idx = min.fIdx;
        frmtCount[idx] = min.frmtCount;
        return idx;
    }

    /**
     * add count to forest with index fIdx, which may not be the forest with
     * minimum frmtCount. Used by fragment count rollback.
     * 
     * @param fIdx
     * @param count
     */
    public void updateStats(int fIdx, long count) {
        synchronized (pq) {
            frmtCount[fIdx] += count;
            Stats tmp = new Stats(fIdx, frmtCount[fIdx]);
            // remove the stats object with the same fIdx
            pq.remove(tmp);
            pq.offer(tmp);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("update forest " + fIdx);
        }
    }



    /**
     * get the index of the forest with smallest number of docs
     */
    @Override
    public int getPlacementForestIndex(DocumentURI uri) {
        int idx = 0;
        try {
            idx = popAndInsert();
        } catch (InterruptedException e) {
            LOG.error("Statistical assignment gets interrupted");
        }
        return idx;
    }

    private class Stats implements Comparable<Stats> {
        int fIdx;
        long frmtCount;

        public Stats(int fIdx, long frmtCount) {
            super();
            this.fIdx = fIdx;
            this.frmtCount = frmtCount;
        }

        @Override
        public int compareTo(Stats o) {
            if (frmtCount > o.frmtCount) {
                return 1;
            } else if (frmtCount < o.frmtCount) {
                return -1;
            } else {
                return 0;
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj == this) {
                return true;
            }
            if (obj.getClass() != getClass()) {
                return false;
            }
            return fIdx == ((Stats) obj).fIdx;
        }
    }

}
