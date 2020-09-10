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
