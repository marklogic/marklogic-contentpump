/*
 * Copyright 2003-2012 MarkLogic Corporation
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.regex.Pattern;

import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentPermission;

public class Utilities {
    private static int BUFFER_SIZE = 32768;
    protected static Pattern[] patterns = new Pattern[] {
            Pattern.compile("&"), Pattern.compile("<"), Pattern.compile(">") };

    public static String escapeXml(String _in) {
        if (null == _in){
            return "";
        }
        return patterns[2].matcher(
                patterns[1].matcher(
                        patterns[0].matcher(_in).replaceAll("&amp;"))
                        .replaceAll("&lt;")).replaceAll("&gt;");
    }
    
    public static String stringArrayToCommaSeparatedString(String [] arrayOfString) {
        StringBuilder result = new StringBuilder();
        for(String string : arrayOfString) {
            result.append(string);
            result.append(",");
        }
        return result.substring(0, result.length() - 1) ;
    }
    public static byte[] cat(InputStream is) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(BUFFER_SIZE);
        copy(is, bos);
        return bos.toByteArray();
    }
    
    public static long copy(InputStream _in, OutputStream _out)
        throws IOException {
        if (_in == null)
            throw new IOException("null InputStream");
        if (_out == null)
            throw new IOException("null OutputStream");

        long totalBytes = 0;
        int len = 0;
        byte[] buf = new byte[BUFFER_SIZE];
        // int available = _in.available();
        // System.err.println("DEBUG: " + _in + ": available " + available);
        while ((len = _in.read(buf, 0, BUFFER_SIZE)) > -1) {
            _out.write(buf, 0, len);
            _out.flush();
            totalBytes += len;
            // System.err.println("DEBUG: " + _out + ": wrote " + len);
        }
        // System.err.println("DEBUG: " + _in + ": last read " + len);

        // caller MUST close the stream for us

        return totalBytes;
    }
    
    public static String cat(Reader r) throws IOException {
        StringBuilder rv = new StringBuilder();

        int size;
        char[] buf = new char[BUFFER_SIZE];
        while ((size = r.read(buf)) > 0) {
            rv.append(buf, 0, size);
        }
        return rv.toString();
    }
    
    public static void validateURI(String uri) {
        if (uri.isEmpty() || 
            Character.isWhitespace(uri.charAt(0)) ||
            Character.isWhitespace(uri.charAt(uri.length() - 1))) {
            throw new IllegalStateException("Invalid URI Format: " + uri);
        }
    }
    
    public static void updateOptionsUsingMeta(ContentCreateOptions options, DocumentMetadata meta) {
        options.setQuality(meta.quality);
        options.setCollections(meta.collectionsList.toArray(new String[0]));
        options.setPermissions(meta.permissionsList.toArray(new ContentPermission[0]));
    }
}
