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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.StatusReporter;
/**
 * Reporter for Content Pump.
 *
 */
public class ContentPumpReporter extends StatusReporter {
    public static final Log LOG = LogFactory.getLog(ContentPumpReporter.class);
    protected Counters counters = new Counters();

    @Override
    public Counter getCounter(Enum<?> name) {
        return counters.findCounter(name);
    }

    @Override
    public Counter getCounter(String group, String name) {
        return counters.findCounter(group, name);
    }

    @Override
    public void progress() {
    }

    @Override
    public void setStatus(String status) {
    }

    @Override
    public float getProgress() {
        return 0;
    }
}
