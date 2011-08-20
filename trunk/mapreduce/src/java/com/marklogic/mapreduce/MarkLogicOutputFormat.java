/*
 * Copyright (c) 2003-2011 MarkLogic Corporation. All rights reserved.
 */
package com.marklogic.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.XSBoolean;

/**
 * MarkLogic-based OutputFormat supeclass. Use the provided subclasses, such as
 * {@link PropertyOutputFormat} to configure your job.
 * 
 * @author jchen
 */
public abstract class MarkLogicOutputFormat<KEYOUT, VALUEOUT> 
extends OutputFormat<KEYOUT, VALUEOUT> 
implements MarkLogicConstants, Configurable {
    public static final Log LOG =
        LogFactory.getLog(MarkLogicOutputFormat.class);
    
    static final String DIRECTORY_TEMPLATE = "{dir}";
    static final String DELETE_DIRECTORY_TEMPLATE = 
        "xdmp:directory-delete(\"" + DIRECTORY_TEMPLATE + "\")";
    static final String CHECK_DIRECTORY_EXIST_TEMPLATE = 
        "exists(xdmp:directory(\"" + DIRECTORY_TEMPLATE + 
        "\", \"infinity\"))";
    static final String DIRECTORY_CREATE_TEMPLATE = 
        "import module namespace admin = \"http://marklogic.com/xdmp/admin\"" +
        " at \"/MarkLogic/admin.xqy\";\n" +
        "let $config := admin:get-configuration()\n" +
        "return admin:database-get-directory-creation($config, " +
        "xdmp:database())";
    static final String MANUAL_DIRECTORY_MODE = "manual";
    
    protected Configuration conf;
    
    protected static String getHost(Configuration conf, int taskId) {
        String[] hosts = conf.getStrings(OUTPUT_HOSTS);
        if (hosts == null || hosts.length == 0) {
            throw new IllegalArgumentException(OUTPUT_HOSTS + 
            " is not specified.");
        }
        return hosts[taskId % hosts.length];
    }
    
    @Override
    public void checkOutputSpecs(JobContext context) throws IOException,
            InterruptedException {
        Session session = null;
        ResultSequence result = null;
        try {
            if (!context.getOutputFormatClass().equals(
                    ContentOutputFormat.class)) {
                return;
            }
            
            String host = getHost(conf, 0);
            URI serverUri = InternalUtilities.getOutputServerUri(conf, host);
            // try getting a connection
            ContentSource cs = InternalUtilities.getOutputContentSource(conf, 
                    serverUri);
            session = cs.newSession();            
            
            // clear output dir if specified
            String outputDir = conf.get(OUTPUT_DIRECTORY);
            if (outputDir != null) {
                outputDir = outputDir.endsWith("/") ? 
                        outputDir : outputDir + "/";
                if (conf.getBoolean(OUTPUT_CLEAN_DIR, false)) {
                    // delete directory if exists
                    String queryText = DELETE_DIRECTORY_TEMPLATE.replace(
                            DIRECTORY_TEMPLATE, outputDir);
                    AdhocQuery query = session.newAdhocQuery(queryText);
                    result = session.submitRequest(query);
                } else { // ensure nothing exists under output dir
                    String queryText = CHECK_DIRECTORY_EXIST_TEMPLATE.replace(
                            DIRECTORY_TEMPLATE, outputDir);
                    AdhocQuery query = session.newAdhocQuery(queryText);
                    result = session.submitRequest(query);
                    if (result.hasNext()) {
                        ResultItem item = result.next();
                        if (((XSBoolean)(item.getItem())).asBoolean()) {
                            throw new IllegalStateException("Directory " + 
                                    outputDir + " already exists");
                        }
                    } else {
                        throw new IOException("Failed to query directory content.");
                    }
                }
            } 
            
            // ensure manual directory creation 
            String queryText = DIRECTORY_CREATE_TEMPLATE;
            AdhocQuery query = session.newAdhocQuery(queryText);
            RequestOptions options = new RequestOptions();
            options.setDefaultXQueryVersion("1.0-ml");
            query.setOptions(options);
            result = session.submitRequest(query);
            if (result.hasNext()) {
                ResultItem item = result.next();
                String dirMode = item.asString();
                if (!dirMode.equals(MANUAL_DIRECTORY_MODE)) {
                    throw new IllegalStateException(
                            "Manual directory creation mode is required. " +
                            "The current creation mode is " + dirMode + ".");
                }
            } else {
                throw new IOException("Failed to query directory creation mode.");
            }
            
            // query forest host mapping
            StringBuilder buf = new StringBuilder();
            buf.append("declare namespace fs=\"");
            buf.append("http://marklogic.com/xdmp/status/forest\";");
            buf.append("for $f in xdmp:database-forests(xdmp:database())");
            buf.append("let $fs := xdmp:forest-status($f)");
            buf.append("return (data($fs//fs:forest-id), ");
            buf.append("xdmp:host-name(data($fs//fs:host-id)))");
            query.setQuery(buf.toString());
            result = session.submitRequest(query);
            LinkedMapWritable forestHostMap = new LinkedMapWritable();
            Text forest = null;
            while (result.hasNext()) {
                ResultItem item = result.next();
                if (forest == null) {
                    forest = new Text(item.asString());            
                } else {
                    Text hostName = new Text(item.asString());
                    forestHostMap.put(forest, hostName);
                    forest = null;
                }
            }
            // store it into config system
            DefaultStringifier.store(conf, forestHostMap, OUTPUT_FOREST_HOST);           
        } catch (URISyntaxException e) {
            throw new IOException(e);
        } catch (XccConfigException e) {
            throw new IOException(e);
        } catch (RequestException e) {
            throw new IOException(e);
        } catch (ClassNotFoundException e) {
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
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context),
                context);
    }
    

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;        
    }
}