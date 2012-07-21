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

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class DocumentPathFilter implements PathFilter, Configurable {

    private String pattern;
    private Configuration conf;
    private boolean pathIsLocal;
    
    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    @Override
    public boolean accept(Path inPath) {
        String filename = inPath.getName();
        if (filename.matches(pattern) == true) {
            return true;
        }
        // take care of the case when INPUT_FILE_PATH is a DIR
        FileSystem fs;
        try {
            if (pathIsLocal == true && !inPath.toString().startsWith("file://")) {
            	//prefix has been lost in LocalFileSystem.globStatusInternal
                inPath = new Path("file://" + inPath.toString());
            }
            fs = inPath.getFileSystem(conf);
            FileStatus[] status = fs.globStatus(inPath);
            for (FileStatus s : status) {
                if (s.isDir()) {
                    return true;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        pattern = conf.get(ConfigConstants.CONF_INPUT_FILE_PATTERN, ".*");
        pathIsLocal = conf.getBoolean(ConfigConstants.CONF_INPUT_FILE_PATH_IS_LOCAL, true);
    }

}
