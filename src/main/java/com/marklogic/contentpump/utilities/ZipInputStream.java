/*
 * Copyright (c) 2011-2016 Progress Software Corporation and/or its subsidiaries or affiliates. All Rights Reserved.
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
/**
 * 
 */
package com.marklogic.contentpump.utilities;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class inherits from java.util.zip.ZipInputStream
 * but override close() method because of a known bug in java
 * JDK-6539065 (http://bugs.java.com/view_bug.do?bug_id=6539065)
 * @author mattsun
 *
 */
public class ZipInputStream extends java.util.zip.ZipInputStream {

    public ZipInputStream(InputStream in) {
        super(in);
        // TODO Auto-generated constructor stub
    }

    @Override
    public void close() throws IOException {
        // Do nothing
    }
    
    public void closeStream() 
            throws IOException {
        super.close();
    }
}
