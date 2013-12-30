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

public class EncodingUtil {
    public static final Log LOG = LogFactory.getLog(EncodingUtil.class);
    /**
     *  Oracle jdk bug 4508058: UTF-8 encoding does not recognize
     *  initial BOM, and it will not be fixed. 
     *  Work Around :
     *  Application code must recognize and skip the BOM itself.
     */
    public static void handleBOMUTF8(String[] vales, int i) {
        byte[] buf = vales[i].getBytes();
        if (LOG.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            for (byte b : buf) {
                sb.append(Byte.toString(b));
                sb.append(" ");
            }
            LOG.debug(vales[i]);
            LOG.debug(sb.toString());
        }
        if (buf[0] == (byte) 0xEF && buf[1] == (byte) 0xBB
            && buf[2] == (byte) 0xBF) {
            vales[i] = new String(buf, 3, buf.length - 3);
        }
    }
}
