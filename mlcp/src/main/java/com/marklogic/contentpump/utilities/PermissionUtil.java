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
package com.marklogic.contentpump.utilities;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.marklogic.mapreduce.LinkedMapWritable;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;
import org.apache.commons.lang.StringUtils;

public class PermissionUtil {
    public static final Log LOG = LogFactory.getLog(PermissionUtil.class);
    public static final String DEFAULT_PERM_QUERY = 
        "for $p in xdmp:default-permissions() " +
        "return ($p/*:role-id/text(),$p/*:capability/text())";
    /**
     * Get a list of ContentPermission fron given string
     * @param perms a string of role-name,capability pais, separated by commna
     * @return
     */
    public static List<ContentPermission> getPermissions(String[] perms) {
        List<ContentPermission> permissions = null;
        if (perms != null && perms.length > 0) {
            int i = 0;
            while (i + 1 < perms.length) {
                String roleName = perms[i++];
                if (StringUtils.isEmpty(roleName)) {
                    LOG.error("Illegal role name: " + roleName);
                    continue;
                }
                String perm = perms[i].trim();
                ContentCapability capability = getCapbility(perm);
                if (capability != null) {
                    if (permissions == null) {
                        permissions = new ArrayList<>();
                    }
                    permissions
                        .add(new ContentPermission(capability, roleName));
                }
                i++;
            }
        }
        return permissions;
    }
    
    
    public static List<ContentPermission> getDefaultPermissions(Configuration conf, LinkedMapWritable roleMap) throws IOException {
        List<ContentPermission> perms = new ArrayList<>();
        Session session = null;
        ResultSequence result = null;
        ContentSource cs;
        try {
            cs = InternalUtilities.getOutputContentSource(conf,
                conf.get(MarkLogicConstants.OUTPUT_HOST));

            session = cs.newSession();
            RequestOptions options = new RequestOptions();
            options.setDefaultXQueryVersion("1.0-ml");

            AdhocQuery query = session.newAdhocQuery(DEFAULT_PERM_QUERY);
            query.setOptions(options);
            result = session.submitRequest(query);
            if (!result.hasNext() || roleMap == null) {
                return null;
            }
            while (result.hasNext()) {
                Text roleid = new Text(result.next().asString());
                if (!result.hasNext()) {
                    throw new IOException("Invalid role,capability pair");
                }
                String roleName = roleMap.get(roleid).toString();
                String cap = result.next().asString();
                ContentCapability capability = PermissionUtil
                    .getCapbility(cap);
                perms.add(new ContentPermission(capability, roleName));
            }
        } catch (XccConfigException e) {
            throw new IOException(e);
        } catch (RequestException e) {
            throw new IOException(e);
        } finally {
            if (result != null) {
                result.close();
            }
            if (session != null) {
                session.close();
            }
        }
        return perms;
    }
    
    public static ContentCapability getCapbility(String cap) {
        ContentCapability capability = null;
        if (ContentCapability.READ.toString().equalsIgnoreCase(cap)) {
            capability = ContentCapability.READ;
        } else if (ContentCapability.EXECUTE.toString().equalsIgnoreCase(cap)) {
            capability = ContentCapability.EXECUTE;
        } else if (ContentCapability.INSERT.toString().equalsIgnoreCase(cap)) {
            capability = ContentCapability.INSERT;
        } else if (ContentCapability.UPDATE.toString().equalsIgnoreCase(cap)) {
            capability = ContentCapability.UPDATE;
        } else {
            LOG.error("Illegal permission: " + cap);
        }
        return capability;
    }
}
