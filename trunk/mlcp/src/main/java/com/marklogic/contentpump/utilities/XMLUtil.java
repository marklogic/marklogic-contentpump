/*
 * Copyright 2003-2014 MarkLogic Corporation
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
package com.marklogic.contentpump.utilities;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sun.org.apache.xml.internal.utils.XMLChar;

public class XMLUtil {
    public static final Log LOG = LogFactory.getLog(XMLUtil.class);
    
    /**
     * Get valid element name from a given string
     * @param name
     * @return
     */
    public static String getValidName(String name) {
        StringBuilder validname = new StringBuilder();
        char ch = name.charAt(0);
        if (!XMLChar.isNameStart(ch)) {
            LOG.warn("Prepend _ to " + name);
            validname.append("_");
        }
        for (int i = 0; i < name.length(); i++) {
            ch = name.charAt(i);
            if (!XMLChar.isName(ch)) {
                LOG.warn("Character " + ch + " in " + name
                    + " is converted to _");
                validname.append("_");
            } else {
                validname.append(ch);
            }
        }

        return validname.toString();
    }
    
    public static String convertToCDATA(String arg) {
        StringBuilder sb = new StringBuilder();
        sb.append("<![CDATA[");
        sb.append(arg);
        sb.append("]]>");
        return sb.toString();
    }
}
