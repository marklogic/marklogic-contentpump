package com.marklogic.contentpump.utilities;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentPermission;

public class PermissionUtil {
    public static final Log LOG = LogFactory.getLog(PermissionUtil.class);
    
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
                if (roleName == null || roleName.isEmpty()) {
                    LOG.error("Illegal role name: " + roleName);
                    continue;
                }
                String perm = perms[i].trim();
                ContentCapability capability = getCapbility(perm);
                if (capability != null) {
                    if (permissions == null) {
                        permissions = new ArrayList<ContentPermission>();
                    }
                    permissions
                        .add(new ContentPermission(capability, roleName));
                }
                i++;
            }
        }
        return permissions;
    }
    
    public static ContentCapability getCapbility(String cap) {
        ContentCapability capability = null;
        if (cap.equalsIgnoreCase(ContentCapability.READ.toString())) {
            capability = ContentCapability.READ;
        } else if (cap.equalsIgnoreCase(ContentCapability.EXECUTE
            .toString())) {
            capability = ContentCapability.EXECUTE;
        } else if (cap.equalsIgnoreCase(ContentCapability.INSERT
            .toString())) {
            capability = ContentCapability.INSERT;
        } else if (cap.equalsIgnoreCase(ContentCapability.UPDATE
            .toString())) {
            capability = ContentCapability.UPDATE;
        } else {
            LOG.error("Illegal permission: " + cap);
        }
        return capability;
    }
}
