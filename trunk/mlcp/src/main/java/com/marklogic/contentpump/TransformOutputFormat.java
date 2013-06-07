/*
 * Copyright 2003-2013 MarkLogic Corporation
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
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.LinkedMapWritable;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;

public class TransformOutputFormat<VALUEOUT> extends
    ContentOutputFormat<VALUEOUT> {
    static final String MIMETYPES_QUERY = "import module namespace hadoop = "
        + "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"
        + "hadoop:get-mimetypes-map()";
    /**
     * internal mimetypes map, initialized in getRecordWriter
     */
    public static LinkedMapWritable mimetypeMap = null;

    /**
     * initialize mimetype map if not initialized, return the map
     * 
     * @return
     * @throws IOException
     */
    private LinkedMapWritable getMimetypesMap() throws IOException {
        if (mimetypeMap != null)
            return mimetypeMap;
        String mtmap = conf.get(ConfigConstants.CONF_MIMETYPES);
        if (mtmap != null) {
            mimetypeMap = DefaultStringifier.load(conf, OUTPUT_FOREST_HOST,
                LinkedMapWritable.class);
            return mimetypeMap;
        }
        String host = conf.get(OUTPUT_HOST);
        Session session = null;
        ResultSequence result = null;
        try {
            ContentSource cs = InternalUtilities.getOutputContentSource(conf,
                host);
            session = cs.newSession();
            AdhocQuery query = session.newAdhocQuery(MIMETYPES_QUERY);
            RequestOptions options = new RequestOptions();
            options.setDefaultXQueryVersion("1.0-ml");
            query.setOptions(options);
            result = session.submitRequest(query);

            mimetypeMap = new LinkedMapWritable();
            while (result.hasNext()) {
                String suffs = result.next().asString();
                Text format = new Text(result.next().asString());
                // some extensions are in a space separated string
                for (String s : suffs.split(" ")) {
                    Text suff = new Text(s);
                    mimetypeMap.put(suff, format);
                }
            }
            return mimetypeMap;
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new IOException(e);
        } finally {
            if (result != null) {
                result.close();
            }
            if (session != null) {
                session.close();
            }
        }
    }

    @Override
    public RecordWriter<DocumentURI, VALUEOUT> getRecordWriter(
        TaskAttemptContext context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        fastLoad = Boolean.valueOf(conf.get(OUTPUT_FAST_LOAD));
        Map<String, ContentSource> sourceMap = getSourceMap(fastLoad, context);
        getMimetypesMap();
        // construct the ContentWriter
        return new TransformWriter<VALUEOUT>(conf, sourceMap, fastLoad, am);
    }

    @Override
    public void checkOutputSpecs(Configuration conf, ContentSource cs)
        throws IOException {
        super.checkOutputSpecs(conf, cs);

        // store mimetypes map into config system
        DefaultStringifier.store(conf, getMimetypesMap(),
            ConfigConstants.CONF_MIMETYPES);
    }

}
