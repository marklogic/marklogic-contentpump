package com.marklogic.mapreduce.utilities;

import java.util.LinkedHashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.marklogic.mapreduce.DocumentURI;


public abstract class AssignmentPolicy {
    public static final Log LOG =
        LogFactory.getLog(AssignmentPolicy.class);
    public enum Kind {LEGACY, BUCKET, RANGE, STATISTICAL};
    protected Kind policy;
    /**
     * updatable forests
     */
    protected LinkedHashSet<String> uForests;
    public Kind getPolicyKind () {
        return policy;
    }
    public abstract String getPlacementForestId(DocumentURI uri);
    public abstract int getPlacementForestIndex(DocumentURI uri);
}
