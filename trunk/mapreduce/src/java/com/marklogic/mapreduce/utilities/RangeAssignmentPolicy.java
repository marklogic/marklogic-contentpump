package com.marklogic.mapreduce.utilities;

import com.marklogic.mapreduce.DocumentURI;

public class RangeAssignmentPolicy extends AssignmentPolicy {
    //MarkLogicOuputFormat should get the partition and the corresponnding forests
    // initialize Range Assignement policy there
    
    /**
     * forests ( RO/DO, retired forests are excluded)
     * can we reuse the forest array in ContentWriter?
     */
    private String[] forests;
    
    @Override
    public String getPlacementForestId(DocumentURI uri) {
        // TODO Auto-generated method stub
        // determine the range based on uri?
        return null;
    }

    @Override
    public int getPlacementForestIndex(DocumentURI uri) {
        // TODO Auto-generated method stub
        return 0;
    }

}
