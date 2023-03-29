/*
 * Copyright (c) 2021 MarkLogic Corporation

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

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The util for restricting hosts capability.
 * The goal of this class is to evenly distribute work load among all hosts.
 *
 * @author mattsun
 *
 */
public class RestrictedHostsUtil {
    public static final Log LOG = LogFactory.getLog(RestrictedHostsUtil.class);
    protected Map<String, HostAssignment> mp;
    protected PriorityQueue<String> q;
    protected List<String> restrictHosts;
    
    /**
     * 
     * @param restrictHosts
     */
    public RestrictedHostsUtil(String[] restrictHosts) {
        this.restrictHosts = Arrays.asList(restrictHosts);
        mp = new HashMap<>();
        q = new PriorityQueue<>(restrictHosts.length,
                new HostAssginmentComparator());
        for (int i = 0; i < restrictHosts.length; i++) {
            mp.put(restrictHosts[i], new HostAssignment(i));
            q.add(restrictHosts[i]);
        }
    }

    public void addForestHost(String hostname) {
        if (restrictHosts.contains(hostname)) {
            mp.get(hostname).assignmentCount++;
            q.remove(hostname);
            q.add(hostname);
        }
    }

    public String getNextHost(String hostName) {
        if (restrictHosts.contains(hostName)) {
            return hostName;
        } else {
            String targetHost = q.poll();
            mp.get(targetHost).assignmentCount++;
            q.add(targetHost);
            return targetHost;
        }
    }

    protected class HostAssignment {
        public int index = 0;
        public int assignmentCount = 0;
        HostAssignment(int index) {
            this.index = index;
        }
    }

    protected class HostAssginmentComparator implements Comparator<String> {
        @Override
        public int compare(String o1, String o2) {
            HostAssignment h1 = mp.get(o1);
            HostAssignment h2 = mp.get(o2);
            if (h1.assignmentCount != h2.assignmentCount) {
                return h1.assignmentCount - h2.assignmentCount;
            } else {
                return h1.index - h2.index;
            }
        }
        
    }
}
