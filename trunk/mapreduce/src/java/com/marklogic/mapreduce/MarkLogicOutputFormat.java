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
import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.XSBoolean;

/**
 * MarkLogic-based OutputFormat superclass. Use the provided subclasses, such
 * as {@link PropertyOutputFormat} to configure your job.
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
    static final String FOREST_HOST_MAP_QUERY =
        "import module namespace hadoop = " +
        "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
        "hadoop:get-forest-host-map()";
    static final String DIRECTORY_CREATE_QUERY = 
        "import module namespace hadoop = " + 
        "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
        "hadoop:get-directory-creation()";
    static final String MANUAL_DIRECTORY_MODE = "manual";
    
    protected Configuration conf;
    
    @Override
    public void checkOutputSpecs(JobContext context) throws IOException,
            InterruptedException {
        InternalUtilities.checkVersion();
        
        Session session = null;
        ResultSequence result = null;
        try {
            String host = conf.get(OUTPUT_HOST);

            if (host == null || host.isEmpty()) {
                throw new IllegalStateException(OUTPUT_HOST +
                        " is not specified.");
            }                     
            
            URI serverUri = InternalUtilities.getOutputServerUri(conf, host);
            // try getting a connection
            ContentSource cs = InternalUtilities.getOutputContentSource(conf, 
                    serverUri);
            session = cs.newSession();   
            
            // query forest host mapping            
            AdhocQuery query = session.newAdhocQuery(FOREST_HOST_MAP_QUERY);
            RequestOptions options = new RequestOptions();
            options.setDefaultXQueryVersion("1.0-ml");
            query.setOptions(options);
            
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
            
            if (!context.getOutputFormatClass().equals(
                    ContentOutputFormat.class)) { 
                return;
            }
            
            // clear output dir if specified
            String outputDir = conf.get(OUTPUT_DIRECTORY);
            if (outputDir != null) {
                outputDir = outputDir.endsWith("/") ? 
                        outputDir : outputDir + "/";
                if (conf.getBoolean(OUTPUT_CLEAN_DIR, false)) {
                    // delete directory if exists
                    String queryText = DELETE_DIRECTORY_TEMPLATE.replace(
                            DIRECTORY_TEMPLATE, outputDir);
                    query.setQuery(queryText);
                    result = session.submitRequest(query);
                } else { // ensure nothing exists under output dir
                    String queryText = CHECK_DIRECTORY_EXIST_TEMPLATE.replace(
                            DIRECTORY_TEMPLATE, outputDir);
                    query.setQuery(queryText);
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
            query.setQuery(DIRECTORY_CREATE_QUERY);
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
            
            // validate capabilities
            String[] perms = conf.getStrings(OUTPUT_PERMISSION);
            if (perms != null && perms.length > 0) {
                if (perms.length % 2 != 0) {
                    throw new IllegalStateException(
                    "Permissions are expected to be in <role, capability> pairs.");
                }
                int i = 0;
                while (i + 1 < perms.length) {
                    String roleName = perms[i++];
                    if (roleName == null || roleName.isEmpty()) {
                        throw new IllegalStateException(
                                "Illegal role name: " + roleName);
                    }
                    String perm = perms[i].trim();
                    if (!perm.equalsIgnoreCase(ContentCapability.READ.toString()) &&
                        !perm.equalsIgnoreCase(ContentCapability.EXECUTE.toString()) &&
                        !perm.equalsIgnoreCase(ContentCapability.INSERT.toString()) &&
                        !perm.equalsIgnoreCase(ContentCapability.UPDATE.toString())) {
                        throw new IllegalStateException("Illegal capability: " + perm);
                    }
                    i++;
                }
            }
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