/*
 * Copyright 2003-2020 MarkLogic Corporation
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
package com.marklogic.contentpump;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * This class returns build information about ContentPump and its dependencies.
 * 
 * @author jchen
 */
public class Versions {

    public static final Log LOG = LogFactory.getLog(ContentPump.class);
    
    private Properties info;
    
    /**
     * Get the ContentPump version.
     * 
     * @return the ContentPump version string, eg. "1.4"
     */
    public static String getVersion() {
        return MLCP_VERSION_INFO._getVersion();
    }
    
    /**
     * Get the minimum supported server version.
     * 
     * @return the minimum supported server version string, eg. "5.0-5"
     */
    public static String getMinServerVersion() {
        return MLCP_VERSION_INFO._getMinServerVersion();
    }
    
    /**
     * Get the maximum supported server version.
     * 
     * @return the minimum supported server version string, eg. "5.0-5"
     */
    public static String getMaxServerVersion() {
        return MLCP_VERSION_INFO._getMaxServerVersion();
    }
    
    private String _getMinServerVersion() {
        return info.getProperty("minServerVersion", "Unknown");
    }
    
    private String _getMaxServerVersion() {
        return info.getProperty("maxServerVersion", "Unknown");
    }

    protected Versions(String component) {
        info = new Properties();
        String versionInfoFile = component + "-version-info.properties";
        InputStream is = null;
        try {
            is = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream(versionInfoFile);
            if (is == null) {
                throw new IOException("Resource not found");
            }
            info.load(is);
        } catch (IOException ex) {
            LogFactory.getLog(getClass()).warn("Could not read '" +
                    versionInfoFile + "', " + ex.toString(), ex);
        } finally {
            IOUtils.closeStream(is);
        }
    }
    
    protected String _getVersion() {
        return info.getProperty("version", "Unknown");
    }
    
    private static Versions MLCP_VERSION_INFO = new Versions("mlcp");
}
