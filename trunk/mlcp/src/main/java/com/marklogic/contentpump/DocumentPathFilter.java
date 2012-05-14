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

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    @Override
    public boolean accept(Path arg0) {
        String filename = arg0.getName();
        int index = filename.lastIndexOf('/');
        filename = filename.substring(index + 1);
        if (filename.matches(pattern) == true) {
            return true;
        }
        // take care of the case when INPUT_FILE_PATH is a DIR
        FileSystem fs;
        try {
            fs = arg0.getFileSystem(conf);
            FileStatus[] status = fs.globStatus(arg0);
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
    }

}
