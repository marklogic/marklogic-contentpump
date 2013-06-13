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

/**
 * Assignment Manager, which is a singleton
 */
public class AssignmentManager {
    static final String ID_PREFIX = "#";
    protected AssignmentPolicy policy;

    private static final AssignmentManager instance = new AssignmentManager();
    private boolean initialized;
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
        if (initialized)
            return;
        else
            initialized = true;
        LinkedHashSet<String> forests = new LinkedHashSet<String>();
        LinkedHashSet<String> updatableForests = new LinkedHashSet<String>();
        for (Writable f : map.keySet()) {
            String fId = ((Text) f).toString();
            ForestInfo fs = (ForestInfo) map.get(f);
            if (fs.getUpdatable()) {
                // updatable
                updatableForests.add(fId);
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

    public void initLegacyPolicy(LinkedHashSet<String> uForests) {
        policy = new LegacyAssignmentPolicy(uForests);
    }

    public int getPlacementForestIndex(DocumentURI uri) {
        return policy.getPlacementForestIndex(uri);
    }
}
