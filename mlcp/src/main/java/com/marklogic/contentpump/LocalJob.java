/*
 * Copyright 2003-2020 MarkLogic Corporation
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import com.marklogic.mapreduce.MarkLogicConstants;

/**
 * A job executed in local mode which manages the job state through the local
 * job engine.
 * 
 * @author jchen
 */
public class LocalJob extends Job implements MarkLogicConstants {
    // Own job state which can be accessed through the local execution engine
    JobStatus.State state;
    
    LocalJob(Configuration conf) throws IOException {
        super(new JobConf(conf));
        state = JobStatus.State.PREP;
    }

    public boolean done() {
        return state != JobStatus.State.RUNNING; 
    }
    
    @Override
    public JobStatus.State getJobState() {
        return state;
    }
    
    public void setJobState(JobStatus.State state) {
        this.state = state;
    }
    
    public static Job getInstance(Configuration conf) throws IOException {
        if (MODE_DISTRIBUTED.equals(conf.get(EXECUTION_MODE))) {
            return Job.getInstance(conf);
        } else {
            return new LocalJob(conf);
        }
    }
}
