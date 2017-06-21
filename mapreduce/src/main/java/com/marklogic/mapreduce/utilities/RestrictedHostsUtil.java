/*
 * Copyright 2003-2017 MarkLogic Corporation
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    protected Map<String,Integer> hostForestCount = new HashMap<>();
    protected List<String> hosts;
    protected int next;
    protected int count;
    /**
     * 
     * @param hosts
     */
    public RestrictedHostsUtil(String[] hosts) {
        this.hosts = Arrays.asList(hosts);
        for (String host: hosts) {
            hostForestCount.put(host, 0);
        }
        next = 0;
        count = 0;
    }
    
    public String getNextHost(String hostName) {
        if (hosts.contains(hostName)) {
            hostForestCount.put(hostName, 
                    hostForestCount.get(hostName).intValue()+1);
            return hostName;
        } else {
            while (hostForestCount.get(hosts.get(next)) > count) {
                // The host is in E-node lists and has been 
                // assigned more than other D-node hosts
                next = ((next+1)%hosts.size());
            }
            String currentHost = hosts.get(next);
            int newCount = hostForestCount.get(currentHost).intValue()+1;
            hostForestCount.put(currentHost, newCount);
            if (newCount >= count) {
                count = newCount;
                next = ((next+1)%hosts.size());
            }
            return currentHost;
        }
    }
}
