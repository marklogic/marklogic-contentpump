package com.marklogic.mapreduce.utilities;

import java.util.LinkedHashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.LinkedMapWritable;

public class AssignmentManager {
    static final String ID_PREFIX = "#";
    protected AssignmentPolicy policy;
    
    private static final AssignmentManager instance = new AssignmentManager();
    
    private AssignmentManager() {}
 
    public static AssignmentManager getInstance() {
        return instance;
    }
    
    public AssignmentPolicy getPolicy() {
        return policy;
    }
    
    public void initialize(AssignmentPolicy.Kind kind, LinkedMapWritable map) {
        //TODO get forests and uForests from map
        LinkedHashSet<String> forests = new LinkedHashSet<String>();
        LinkedHashSet<String> updatableForests = new LinkedHashSet<String>();
//        LinkedHashSet<LongWritable> docCount = new LinkedHashSet<LongWritable>();
        for (Writable f :  map.keySet()) {
            String fId = ((Text)f).toString();
            ForestStatus fs = (ForestStatus)map.get(f);
            if (fs.getUpdatable().get()) {
                //updatable
                updatableForests.add(fId);
//                docCount.add(fs.getDocCount());
            }
            forests.add(fId);
        }
        switch(kind) {
        case BUCKET:
            initBucketPolicy(forests.toArray(new String[forests.size()]),
                updatableForests);
            break;
        case LEGACY:
            initLegacyPolicy(updatableForests);
            break;
        case RANGE:
            break;
        case STATISTICAL:
//            LongWritable [] countLongAry = docCount.toArray(new LongWritable[docCount.size()]);
            String[] uForests = updatableForests.toArray(new String[updatableForests.size()]);
            long [] countAry = new long[updatableForests.size()];
            for(int i=0; i<countAry.length; i++) {
                countAry[i] = ((ForestStatus)map.get(new Text(uForests[i]))).getDocCount().get();
            }
            initStatisticalPolicy(countAry, updatableForests);
            break;
        }
    }
    
    public void initBucketPolicy(String[] forests, LinkedHashSet<String> uForests) {
        policy = new BucketAssignmentPolicy(forests,uForests);
    }
    
    public void initRangePolicy() {
        
    }
    
    public void initStatisticalPolicy(long[] docCount, LinkedHashSet<String> uForests) {
        policy = new StatisticalAssignmentPolicy(docCount, uForests);
    }
    
    public void initLegacyPolicy(LinkedHashSet<String> uForests) {
        policy = new LegacyAssignmentPolicy(uForests);
    }
    
    /**
     * this will be called each doc is inserted
     * @param uri
     * @return
     */
    
    // TODO maybe only statistical needs synchronization because stats get updated real-time?
    // bucket table is read only
    public String getPlacementForestId(DocumentURI uri) {
        return policy.getPlacementForestId(uri);
    }
    
    public int getPlacementForestIndex(DocumentURI uri) {
        return policy.getPlacementForestIndex(uri);
    }
}
