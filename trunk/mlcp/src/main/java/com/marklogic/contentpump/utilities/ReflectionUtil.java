/*
 * Copyright 2003-2013 MarkLogic Corporation
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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

/**
 * Utility class for creating various instances using reflection.
 * 
 * @author jchen
 *
 */
public class ReflectionUtil {  
    @SuppressWarnings("unchecked")
    public static TaskAttemptContext createTaskAttemptContext(
            Configuration conf, TaskAttemptID taskAttemptId) throws Exception {
        Class<TaskAttemptContext> contextClass = TaskAttemptContext.class;
        int mods = contextClass.getModifiers();
        Class[] types = new Class[2];
        types[0] = Configuration.class;
        types[1] = TaskAttemptID.class;
        Object[] params = new Object[2];
        params[0] = conf;
        params[1] = taskAttemptId;
        if (Modifier.isAbstract(mods)) { // Hadoop 2.0
            Class contextImplClass = Class.forName(
                    "org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
            Constructor contextCtr = contextImplClass.getConstructor(types);
            return (TaskAttemptContext)contextCtr.newInstance(params);
        } else {           
            Constructor<TaskAttemptContext> contextCtr = 
                (Constructor<TaskAttemptContext>) 
                contextClass.getConstructor(types); 
            return (TaskAttemptContext) contextCtr.newInstance(params);
        }
    }
    
    @SuppressWarnings("unchecked")
    public static Mapper.Context createMapperContext(Mapper mapper, 
            Configuration conf, TaskAttemptID taskAttemptId, 
            RecordReader reader, RecordWriter writer, 
            OutputCommitter committer, StatusReporter reporter, 
            InputSplit split) throws Exception {
        Class<Mapper.Context> contextClass = Mapper.Context.class;
        int mods = contextClass.getModifiers();
        if (Modifier.isAbstract(mods)) { // Hadoop 2.0
            Class[] types = new Class[7];
            types[0] = Configuration.class;
            types[1] = TaskAttemptID.class;
            types[2] = RecordReader.class;
            types[3] = RecordWriter.class;
            types[4] = OutputCommitter.class;
            types[5] = StatusReporter.class;
            types[6] = InputSplit.class;
            Object[] params = new Object[7];
            params[0] = conf;
            params[1] = taskAttemptId;
            params[2] = reader;
            params[3] = writer;
            params[4] = committer;
            params[5] = reporter;
            params[6] = split;
            Class contextImplClass = Class.forName(
                    "org.apache.hadoop.mapreduce.task.MapContextImpl");
            Constructor contextImplCtr = 
                contextImplClass.getConstructor(types);
            Object mapContextImpl = contextImplCtr.newInstance(params);
            Class mapperClass = Class.forName(
                "org.apache.hadoop.mapreduce.lib.map.WrappedMapper");
            Object wrappedMapper = mapperClass.newInstance();
            Class[] contextTypes = new Class[1];
            contextTypes[0] = MapContext.class;
            Method getMapContext = mapperClass.getMethod("getMapContext", 
                    contextTypes);
            Object[] contextParams = new Object[1];
            contextParams[0] = mapContextImpl;
            return (Mapper.Context)getMapContext.invoke(wrappedMapper, 
                    contextParams);
        } else {
            Class[] types = new Class[8];
            types[0] = Mapper.class;
            types[1] = Configuration.class;
            types[2] = TaskAttemptID.class;
            types[3] = RecordReader.class;
            types[4] = RecordWriter.class;
            types[5] = OutputCommitter.class;
            types[6] = StatusReporter.class;
            types[7] = InputSplit.class;
            Object[] params = new Object[8];
            params[0] = mapper;
            params[1] = conf;
            params[2] = taskAttemptId;
            params[3] = reader;
            params[4] = writer;
            params[5] = committer;
            params[6] = reporter;
            params[7] = split;
            
            Constructor<Mapper.Context>[] contextCtr = 
            (Constructor<Mapper.Context>[])contextClass.getConstructors();
            return contextCtr[0].newInstance(params);
        }
    }
}
