/*
 * Copyright 2003-2016 MarkLogic Corporation

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
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

import com.marklogic.contentpump.utilities.AuditUtil;
import com.marklogic.contentpump.ConfigConstants;
import com.marklogic.mapreduce.MarkLogicInputFormat;
import com.marklogic.xcc.ResultItem;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.types.ItemType;
import com.marklogic.xcc.types.XSString;

/**
 * Extension of {@link MarkLogicInputFormat} and 
 * a generic super class for all MarkLogic-based InputFormat
 * in Content Pump.
 * 
 * @author mattsun
 *
 */
public class DocumentInputFormat<VALUEIN> 
extends com.marklogic.mapreduce.DocumentInputFormat<VALUEIN> {
    boolean mlcpStartEventEnabled = false;
    boolean mlcpFinishEventEnabled = false;
    
    protected void appendCustom(StringBuilder buf) {
        buf.append("\"AUDIT\",\n");
        buf.append("let $group-id := xdmp:group()\n");
        buf.append("let $enabled-event := xdmp:group-get-audit-event-type-enabled($group-id,(\"mlcp-start\", \"mlcp-finish\"))\n");
        buf.append("let $mlcp-start-enabled := if ($enabled-event[1]) then \"mlcp-start\" else ()\n");
        buf.append("let $mlcp-finish-enabled := if ($enabled-event[2]) then \"mlcp-finish\" else ()\n");
        buf.append("return ($mlcp-start-enabled, $mlcp-finish-enabled)");
    }
    
    protected void getForestSplits(JobContext jobContext,
            ResultSequence result, 
            List<ForestSplit> forestSplits,
            List<String> ruleUris) throws IOException {
        Configuration jobConf = jobContext.getConfiguration();
        super.getForestSplits(jobContext, result, forestSplits, ruleUris);
        // Third while loop: audit settings
        while (result.hasNext()) {
            ResultItem item = result.next();
            if (ItemType.XS_STRING != item.getItemType()) {
                throw new IOException("Unexpected item type " + item.getItemType().toString());
            }
            String itemStr = ((XSString)item.getItem()).asString();
            if ("AUDIT".equals(itemStr)) {
                continue;
            } else if ("mlcp-start".equals(itemStr)) {
                mlcpStartEventEnabled = true;
            } else if ("mlcp-finish".equalsIgnoreCase(itemStr)) {
                mlcpFinishEventEnabled = true;
            } else {
                throw new IOException("Unrecognized audit event " + itemStr);
            }                
        }
        if (ruleUris.size() > 0) {
            AuditUtil.prepareAuditMlcpFinish(jobConf, ruleUris.size());
            if (LOG.isDebugEnabled()) {
                // TODO: Use this version if only JAVA 8 is supported
                // String logMessage = String.join(", ", ruleUris);
                LOG.debug("Redaction rules applied: " + StringUtils.join(ruleUris, ", "));
            }
        }
        if (mlcpStartEventEnabled) {
            AuditUtil.auditMlcpStart(jobConf, jobContext.getJobName());
        }
        jobConf.setBoolean(ConfigConstants.CONF_AUDIT_MLCPFINISH_ENABLED, mlcpFinishEventEnabled);
    }
}
