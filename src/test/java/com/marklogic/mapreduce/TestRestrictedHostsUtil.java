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

package com.marklogic.mapreduce;

import static org.junit.Assert.*;

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

        protected int getAssignment(String hostname) {
            return this.mp.get(hostname).assignmentCount;
        }
        
    }

    public MockRestrictedHostsUtil rhUtilSetup() {
        String[] enodes = {"host-1","host-2","host-3","host-4"};
        MockRestrictedHostsUtil rhUtil =
                new MockRestrictedHostsUtil(enodes);
        return rhUtil;
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }
    
    /**
     * Test method for {@link com.marklogic.mapreduce.utilities.RestrictedHostsUtil#getNextHost(java.lang.String)}.
     */
    @Test
    public void testGetNextHost1() {
        MockRestrictedHostsUtil rhUtil = rhUtilSetup();
        String host = "host-2";
        for (int i = 0; i < 10; i++) {
            rhUtil.addForestHost(host);
        }
        for (int i = 0; i < 10; i++) {
            String nextHost = rhUtil.getNextHost(host);
            assertTrue(nextHost.equalsIgnoreCase(host));
        }
        String key = "{host-4=0, host-3=0, host-2=10, host-1=0}";
        assertTrue(rhUtil.getAssignment("host-1")==0);
        assertTrue(rhUtil.getAssignment("host-2")==10);
        assertTrue(rhUtil.getAssignment("host-3")==0);
        assertTrue(rhUtil.getAssignment("host-4")==0);
    }

    /**
     * Test method for {@link com.marklogic.mapreduce.utilities.RestrictedHostsUtil#getNextHost(java.lang.String)}.
     */
    @Test
    public void testGetNextHost2() {
        MockRestrictedHostsUtil rhUtil = rhUtilSetup();
        String host = "host-5";
        for (int i = 0; i < 10; i++) {
            rhUtil.getNextHost(host);
        }
        String key = "{host-4=2, host-3=2, host-2=3, host-1=3}";
        assertTrue(rhUtil.getAssignment("host-1")==3);
        assertTrue(rhUtil.getAssignment("host-2")==3);
        assertTrue(rhUtil.getAssignment("host-3")==2);
        assertTrue(rhUtil.getAssignment("host-4")==2);
        
    }
    
    /**
     * Test method for {@link com.marklogic.mapreduce.utilities.RestrictedHostsUtil#getNextHost(java.lang.String)}.
     */
    @Test
    public void testGetNextHost3() {
        MockRestrictedHostsUtil rhUtil = rhUtilSetup();
        
        String[] hosts = {"host-5","host-4","host-7",
                "host-10","host-4","host-2","host-2",
                "host-2","host-2","host-2","host-2"};

        for (String host : hosts) {
            rhUtil.addForestHost(host);
        }
        for (String host: hosts) {
            rhUtil.getNextHost(host);
        }

        String key = "{host-4=2, host-3=1, host-2=6, host-1=2}";
        assertTrue(rhUtil.getAssignment("host-1")==2);
        assertTrue(rhUtil.getAssignment("host-2")==6);
        assertTrue(rhUtil.getAssignment("host-3")==1);
        assertTrue(rhUtil.getAssignment("host-4")==2);
        
    }
    
    /**
     * for bug:43647
     * Test method for {@link com.marklogic.mapreduce.utilities.RestrictedHostsUtil#getNextHost(java.lang.String)}.
     */
    @Test
    public void testGetNextHost4() {
        MockRestrictedHostsUtil rhUtil = rhUtilSetup();
        
        String[] hosts = {"host-1","host-2","host-3",
                "host-4","host-5"};

        for (String host : hosts) {
            rhUtil.addForestHost(host);
        }
        for (String host: hosts) {
            rhUtil.getNextHost(host);
        }

        String key = "{host-4=1, host-3=1, host-2=1, host-1=2}";
        assertTrue(rhUtil.getAssignment("host-1")==2);
        assertTrue(rhUtil.getAssignment("host-2")==1);
        assertTrue(rhUtil.getAssignment("host-3")==1);
        assertTrue(rhUtil.getAssignment("host-4")==1);
        
    }
    
    /**
     * Test method for {@link com.marklogic.mapreduce.utilities.RestrictedHostsUtil#getNextHost(java.lang.String)}.
     */
    @Test
    public void testGetNextHost5() {
        MockRestrictedHostsUtil rhUtil = rhUtilSetup();
        
        String[] hosts = {"host-5","host-5","host-5",
                "host-5","host-5","host-1"};

        for (String host : hosts) {
            rhUtil.addForestHost(host);
        }
        for (String host: hosts) {
            rhUtil.getNextHost(host);
        }

        String key = "{host-4=1, host-3=1, host-2=2, host-1=2}";
        assertTrue(rhUtil.getAssignment("host-1")==2);
        assertTrue(rhUtil.getAssignment("host-2")==2);
        assertTrue(rhUtil.getAssignment("host-3")==1);
        assertTrue(rhUtil.getAssignment("host-4")==1);
        
    }
    
}
