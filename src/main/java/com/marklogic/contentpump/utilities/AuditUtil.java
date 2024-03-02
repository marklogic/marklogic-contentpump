/*
 * Copyright (c) 2023 MarkLogic Corporation
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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;

import com.marklogic.contentpump.ConfigConstants;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import com.marklogic.xcc.types.ValueType;

/**
 * @author mattsun
 *
 */
public class AuditUtil {
    public static final Log LOG = LogFactory.getLog(AuditUtil.class);
    
    /**
     * @param conf
     * @param ruleCount
     */
    public static void prepareAuditMlcpFinish(Configuration conf, int ruleCount) {
        StringBuilder buf = new StringBuilder();
        buf.append("(Redaction Counters) Applicable rules found=");
        buf.append(ruleCount);
        conf.set(ConfigConstants.CONF_AUDIT_MLCPFINISH_MESSAGE, buf.toString());
    }
    
    /**
     * @param conf
     * @param jobName
     * @param counters
     * @throws IOException
     */
    public static void auditMlcpFinish(Configuration conf, String jobName, 
            Counters counters) throws IOException {
        boolean success = false;
        if (!conf.getBoolean(ConfigConstants.CONF_AUDIT_MLCPFINISH_ENABLED, false)) {
            return;
        }
        
        StringBuilder auditBuf = new StringBuilder();
        auditBuf.append("job=");
        auditBuf.append(jobName);
        
        if (counters.countCounters() > 0) {
            auditBuf.append(";");
            Iterator<CounterGroup> groupIt = counters.iterator();
            int groupCounter = 0;
            while (groupIt.hasNext()) {
                CounterGroup group = groupIt.next();
                if (groupCounter != 0) {
                    auditBuf.append("; ");
                } else {
                    auditBuf.append(" ");
                }
                
                auditBuf.append('(');
                auditBuf.append(group.getDisplayName());
                auditBuf.append(") ");

                Iterator<Counter> counterIt = group.iterator();
                int counterCount = 0;
                while (counterIt.hasNext()) {
                    if (counterCount != 0) {
                        auditBuf.append(", ");
                    }
                    Counter counter = counterIt.next();                    
                    auditBuf.append(counter.getDisplayName());
                    auditBuf.append('=');
                    auditBuf.append(counter.getValue());
                    counterCount++;
                }
                groupCounter++;
            }
            success = true;
        }
        
        String ruleCounter = conf.get(ConfigConstants.CONF_AUDIT_MLCPFINISH_MESSAGE);
        if (ruleCounter != null) {
            auditBuf.append("; ");
            auditBuf.append(ruleCounter);
        }
        String auditMessage = auditBuf.toString();
        
        auditBuf = new StringBuilder();
        auditBuf.append("xquery version \"1.0-ml\";\n");
        auditBuf.append("declare variable $success as xs:boolean external;\n");
        auditBuf.append("xdmp:audit(\"");
        auditBuf.append(ConfigConstants.AUDIT_MLCPFINISH_CODE);
        auditBuf.append("\",\"");
        auditBuf.append(auditMessage);
        auditBuf.append("\", ");
        auditBuf.append("xdmp:get-current-user(), $success)");
        String auditQueryStr = auditBuf.toString();
        
        Session auditSession = null;
        ContentSource auditCs = null;
        try {
            auditCs = InternalUtilities.getInputContentSource(conf);
            auditSession = auditCs.newSession();
            RequestOptions options = new RequestOptions();
            options.setCacheResult(false);

            AdhocQuery auditQuery = auditSession.newAdhocQuery(auditQueryStr);
            auditQuery.setOptions(options);
            auditQuery.setNewVariable("success", ValueType.XS_BOOLEAN, 
                    new Boolean(success));
            auditSession.submitRequest(auditQuery);
        } catch (XccConfigException e) {
            LOG.error(e);
            throw new IOException(e);
        } catch (URISyntaxException e) {
            LOG.error(e);
            throw new IOException(e);
        } catch (RequestException e) {
            LOG.error(e);
            LOG.error("Query: " + auditQueryStr);
            throw new IOException(e);
        }
    }
    
    /**
     * @param job
     * @param cmd
     * @param cmdline
     */
    public static void prepareAuditMlcpStart(Job job, String cmd, 
            CommandLine cmdline) {
        Configuration conf = job.getConfiguration();
        StringBuilder buf = new StringBuilder();
        buf.append(cmd);
        buf.append(" ");
        Option[] options = cmdline.getOptions();
        for (int i = 0; i < options.length; i++) {
            String name = options[i].getOpt();
            // Hide password from command
            if (name.matches(".*password")) {
                continue;
            }
            if (i != 0) {
                buf.append(' ');
            }
            buf.append('-');
            buf.append(name);
            String value = cmdline.getOptionValue(name);
            if (value != null) {
                buf.append(' ');
                buf.append(value);
            }
        }
        
        conf.set(ConfigConstants.CONF_AUDIT_MLCPSTART_MESSAGE, 
                buf.toString());
    }
    
    /**
     * @param conf
     * @param jobName
     * @throws IOException
     */
    public static void auditMlcpStart(Configuration conf, String jobName) 
            throws IOException {
        String mlcpStartMessage = conf.get(ConfigConstants.CONF_AUDIT_MLCPSTART_MESSAGE);
        StringBuilder auditBuf = new StringBuilder();
        auditBuf.append("job=");
        auditBuf.append(jobName);
        auditBuf.append("; ");
        if (mlcpStartMessage != null) {
            auditBuf.append(mlcpStartMessage);
        }
        String auditMessage = auditBuf.toString(); 
        auditBuf = new StringBuilder();
        
        if (auditMessage != null) {                
            auditBuf.append("xquery version \"1.0-ml\";\n");
            auditBuf.append("xdmp:audit(\"");
            auditBuf.append(ConfigConstants.AUDIT_MLCPSTART_CODE);
            auditBuf.append("\",\"");
            auditBuf.append(auditMessage);
            auditBuf.append("\", xdmp:get-current-user())");
            String auditQueryStr = auditBuf.toString();
            
            Session auditSession = null;
            ContentSource auditCs = null;
            try {
                auditCs = InternalUtilities.getInputContentSource(conf);
                auditSession = auditCs.newSession();
                RequestOptions options = new RequestOptions();
                options.setCacheResult(false);

                AdhocQuery auditQuery = auditSession.newAdhocQuery(auditQueryStr);
                auditQuery.setOptions(options);
                auditSession.submitRequest(auditQuery);
            } catch (XccConfigException e) {
                LOG.error(e);
                throw new IOException(e);
            } catch (URISyntaxException e) {
                LOG.error(e);
                throw new IOException(e);
            } catch (RequestException e) {
                LOG.error(e);
                LOG.error("Query: " + auditQueryStr);
                throw new IOException(e);
            }
        }            
    }
}
