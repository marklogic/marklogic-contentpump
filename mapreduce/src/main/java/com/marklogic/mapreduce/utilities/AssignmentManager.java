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

import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.LinkedMapWritable;

/**
 * Assignment Manager, which is a singleton
 */
public class AssignmentManager {
    static final String ID_PREFIX = "#";
    protected AssignmentPolicy policy;

    private static final AssignmentManager instance = new AssignmentManager();
    private boolean initialized;
    /* 
     * a list of master forest ids
     */
    private String[] masterIds;
    /* 
     * mapping from master forest to a list of replica forests
     */
    private LinkedHashMap<String, ArrayList<String> > replicaMap;

    private AssignmentManager() {
        initialized = false;
    }

    public static AssignmentManager getInstance() {
        return instance;
    }

    public synchronized AssignmentPolicy getPolicy() {
        return policy;
    }

    public synchronized void initialize(AssignmentPolicy.Kind kind,
        LinkedMapWritable map, int batchSize) {
        if (initialized) {
            return;
        } else {
            initialized = true;
        }
        LinkedHashSet<String> forests = new LinkedHashSet<String>();
        LinkedHashSet<String> updatableForests = new LinkedHashSet<String>();
        replicaMap = new LinkedHashMap<String, ArrayList<String> >();
        for (Writable f : map.keySet()) {
            String fId = ((Text) f).toString();
            ForestInfo fs = (ForestInfo) map.get(f);
            String masterId = fs.getMasterId();
            ArrayList<String> replicas = replicaMap.get(masterId);
            if (replicas == null) {
              replicas = new ArrayList<String>();
              replicas.add(fId);
              replicaMap.put(masterId,replicas);
            } else {
              replicas.add(fId);
            }
            if (!fId.equals(fs.getMasterId())) {
              continue;
            }
            if (fs.getUpdatable()) {
                // updatable
                updatableForests.add(fId);
            }
            forests.add(fId);
        }
        masterIds = forests.toArray(new String[forests.size()]);
        switch (kind) {
        case BUCKET:
            initBucketPolicy(forests.toArray(new String[forests.size()]),
                updatableForests);
            break;
        case LEGACY:
            initLegacyPolicy(updatableForests);
            break;
        case RANGE: {
            String[] uForests = updatableForests
                .toArray(new String[updatableForests.size()]);
            long[] countAry = new long[updatableForests.size()];
            for (int i = 0; i < countAry.length; i++) {
                countAry[i] = ((ForestInfo) map.get(new Text(uForests[i])))
                    .getFragmentCount();
            }
            initRangePolicy(countAry, batchSize);
        }
            break;
        case STATISTICAL: {
            String[] uForests = updatableForests
                .toArray(new String[updatableForests.size()]);
            long[] countAry = new long[updatableForests.size()];
            for (int i = 0; i < countAry.length; i++) {
                countAry[i] = ((ForestInfo) map.get(new Text(uForests[i])))
                    .getFragmentCount();
            }
            initStatisticalPolicy(countAry, batchSize);
        }
            break;
        case QUERY: {
            String[] uForests = updatableForests
                .toArray(new String[updatableForests.size()]);
            long[] countAry = new long[updatableForests.size()];
            for (int i = 0; i < countAry.length; i++) {
                countAry[i] = ((ForestInfo) map.get(new Text(uForests[i])))
                    .getFragmentCount();
            }
            initQueryPolicy(countAry, batchSize);
        }
            break;
        }
    }

    public void initBucketPolicy(String[] forests,
        LinkedHashSet<String> uForests) {
        policy = new BucketAssignmentPolicy(forests, uForests);
    }

    public void initRangePolicy(long[] docCount, int batchSize) {
        policy = new RangeAssignmentPolicy(docCount, batchSize);
    }

    public void initStatisticalPolicy(long[] docCount, int batchSize) {
        policy = new StatisticalAssignmentPolicy(docCount, batchSize);
    }

    public void initQueryPolicy(long[] docCount, int batchSize) {
        policy = new QueryAssignmentPolicy(docCount, batchSize);
    }

    public void initLegacyPolicy(LinkedHashSet<String> uForests) {
        policy = new LegacyAssignmentPolicy(uForests);
    }

    public int getPlacementForestIndex(DocumentURI uri) {
        return policy.getPlacementForestIndex(uri);
    }

    public String[] getMasterIds() {
        return masterIds;
    }

    public ArrayList<String> getReplicas(String id) {
        return replicaMap.get(id);
    }
    
    /**
     * Used internally for testing
     * @param val
     */
    public void setInitialized(boolean val) {
        initialized = val;
    }
}
