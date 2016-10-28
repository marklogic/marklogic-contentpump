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

package com.marklogic;

import static org.junit.Assert.*;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.marklogic.mapreduce.utilities.RestrictedHostsUtil;

/**
 * @author mattsun
 *
 */
public class TestRestrictedHostsUtil {
    
    class MockRestrictedHostsUtil extends RestrictedHostsUtil {

        public MockRestrictedHostsUtil(String[] hosts) {
            super(hosts);
        }
        
        protected Map<String, Integer> getMap() {
            return this.hostForestCount;
        }
        
        protected void reset() {
            next = 0;
            count = 0;
            for (String host : hosts) {
                this.hostForestCount.put(host, 0);
            }
        }
    }
    MockRestrictedHostsUtil rhUtil;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        String[] enodes = {"host-1","host-2","host-3","host-4"};
        rhUtil = new MockRestrictedHostsUtil(enodes);
    }
    
    /**
     * Test method for {@link com.marklogic.contentpump.utilities.RestrictedHostsUtil#getNextHost(java.lang.String)}.
     */
    @Test
    public void testGetNextHost1() {
        rhUtil.reset();
        String host = "host-2";
        for (int i = 0; i < 10; i++) {
            String nextHost = rhUtil.getNextHost(host);
            assertTrue(nextHost.equalsIgnoreCase(host));
        }
        String key = "{host-4=0, host-3=0, host-2=10, host-1=0}";
        assertTrue(key.equals(rhUtil.getMap().toString()));
        
    }

    /**
     * Test method for {@link com.marklogic.contentpump.utilities.RestrictedHostsUtil#getNextHost(java.lang.String)}.
     */
    @Test
    public void testGetNextHost2() {
        rhUtil.reset();
        String host = "host-5";
        for (int i = 0; i < 10; i++) {
            rhUtil.getNextHost(host);
        }
        String key = "{host-4=2, host-3=2, host-2=3, host-1=3}";
        assertTrue(key.equals(rhUtil.getMap().toString()));
        
    }
    
    /**
     * Test method for {@link com.marklogic.contentpump.utilities.RestrictedHostsUtil#getNextHost(java.lang.String)}.
     */
    @Test
    public void testGetNextHost3() {
        rhUtil.reset();
        
        rhUtil.getNextHost("host-2");
        rhUtil.getNextHost("host-5");
        rhUtil.getNextHost("host-2");
        rhUtil.getNextHost("host-4");
        rhUtil.getNextHost("host-7");
        rhUtil.getNextHost("host-2");
        rhUtil.getNextHost("host-2");
        rhUtil.getNextHost("host-4");
        rhUtil.getNextHost("host-2");
        rhUtil.getNextHost("host-2");
        rhUtil.getNextHost("host-10");

        String key = "{host-4=2, host-3=1, host-2=6, host-1=2}";
        assertTrue(key.equals(rhUtil.getMap().toString()));
        
    }
    
}
