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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.LinkedMapWritable;

public class AssignmentManager {
    static final String ID_PREFIX = "#";
    protected AssignmentPolicy policy;
    
    private static final AssignmentManager instance = new AssignmentManager();
    
    private AssignmentManager() {}
 
    public static AssignmentManager getInstance() {
        return instance;
    }
    
    public AssignmentPolicy getPolicy() {
        return policy;
    }
    
    public void initialize(AssignmentPolicy.Kind kind, LinkedMapWritable map) {
        //TODO get forests and uForests from map
        LinkedHashSet<String> forests = new LinkedHashSet<String>();
        LinkedHashSet<String> updatableForests = new LinkedHashSet<String>();
//        LinkedHashSet<LongWritable> docCount = new LinkedHashSet<LongWritable>();
        for (Writable f :  map.keySet()) {
            String fId = ((Text)f).toString();
            ForestStatus fs = (ForestStatus)map.get(f);
            if (fs.getUpdatable().get()) {
                //updatable
                updatableForests.add(fId);
//                docCount.add(fs.getDocCount());
            }
            forests.add(fId);
        }
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
                countAry[i] = ((ForestStatus) map.get(new Text(uForests[i])))
                    .getDocCount().get();
            }
            initRangePolicy(countAry, updatableForests);
        }
            break;
        case STATISTICAL: {
            String[] uForests = updatableForests
                .toArray(new String[updatableForests.size()]);
            long[] countAry = new long[updatableForests.size()];
            for (int i = 0; i < countAry.length; i++) {
                countAry[i] = ((ForestStatus) map.get(new Text(uForests[i])))
                    .getDocCount().get();
            }
            initStatisticalPolicy(countAry, updatableForests);
        }
            break;
        }
    }
    
    public void initBucketPolicy(String[] forests, LinkedHashSet<String> uForests) {
        policy = new BucketAssignmentPolicy(forests,uForests);
    }
    
    public void initRangePolicy(long[] docCount, LinkedHashSet<String> uForests) {
        policy = new RangeAssignmentPolicy(docCount, uForests);
    }
    
    public void initStatisticalPolicy(long[] docCount, LinkedHashSet<String> uForests) {
        policy = new StatisticalAssignmentPolicy(docCount, uForests);
    }
    
    public void initLegacyPolicy(LinkedHashSet<String> uForests) {
        policy = new LegacyAssignmentPolicy(uForests);
    }
    
    /**
     * this will be called each doc is inserted
     * @param uri
     * @return
     */
    
    // TODO maybe only statistical needs synchronization because stats get updated real-time?
    // bucket table is read only
    public String getPlacementForestId(DocumentURI uri) {
        return policy.getPlacementForestId(uri);
    }
    
    public int getPlacementForestIndex(DocumentURI uri) {
        return policy.getPlacementForestIndex(uri);
    }
}
