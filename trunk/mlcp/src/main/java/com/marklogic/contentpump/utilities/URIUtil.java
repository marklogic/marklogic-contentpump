/*******************************************************************************
 * Copyright 2003-2013 MarkLogic Corporation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.marklogic.contentpump.utilities;

import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.marklogic.contentpump.ConfigConstants;
import com.marklogic.mapreduce.DocumentURI;

/**
 * Class containing utility functions for URI manipulations.
 * 
 * @author jchen
 */
public class URIUtil {
    public static final Log LOG = LogFactory.getLog(URIUtil.class);

    /**
     * Apply URI replacement configuration option to a URI source string.  The
     * configuration option is a list of comma separated pairs of regex 
     * patterns and replacements.  Validation of the configuration is done at
     * command parsing time.
     * 
     * @param uriSource
     * @param conf
     * @return result URI string
     */
    public static String applyUriReplace(String uriSource, Configuration conf) {
        if (uriSource == null) return null;
        String[] uriReplace = conf.getStrings(
                ConfigConstants.CONF_OUTPUT_URI_REPLACE);
        if (uriReplace == null) return uriSource;
        for (int i = 0; i < uriReplace.length - 1; i += 2) {
            String replacement = uriReplace[i+1].trim();
            replacement = replacement.substring(1, replacement.length()-1);
            uriSource = uriSource.replaceAll(uriReplace[i], replacement);
        }
        return uriSource;
    }
    
    /**
     * Apply URI prefix and suffix configuration option to a URI source string.
     * 
     * @param uriSource
     * @param conf
     * @return result URI string
     */
    public static String applyPrefixSuffix(String uriSource, 
            Configuration conf) {
        if (uriSource == null) return null;
        String prefix = conf.get(ConfigConstants.CONF_OUTPUT_URI_PREFIX);
        String suffix = conf.get(ConfigConstants.CONF_OUTPUT_URI_SUFFIX);
        if (prefix == null && suffix == null) {
            return uriSource;
        }
        int len = uriSource.length() +
                (prefix != null ? prefix.length() : 0) +
                (suffix != null ? suffix.length() : 0);
        StringBuilder uriBuf = new StringBuilder(len);
        if (prefix != null) {
            uriBuf.append(prefix);
        }
        uriBuf.append(uriSource);
        if (suffix != null) {
            uriBuf.append(suffix);
        }
        return uriBuf.toString();
    }
    
    public static String getPathFromURI(DocumentURI uri)  {
        String uriStr = uri.getUri();
        try {
            URI child = new URI(uriStr);
            String childPath;
            if (child.isOpaque()) {
                childPath = child.getSchemeSpecificPart();
            } else {
                childPath = child.getPath();
            }
            return childPath;
        } catch (Exception ex) {
            LOG.warn("Error parsing URI " + uriStr + ".");
            return uriStr;
        }
    }
}
