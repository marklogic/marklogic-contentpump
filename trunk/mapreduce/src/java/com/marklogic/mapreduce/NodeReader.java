/*
 * Copyright 2003-2014 MarkLogic Corporation

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
package com.marklogic.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.marklogic.xcc.ResultItem;

/**
 * MarkLogicRecordReader for nodes where the key is NodePath.
 * @author jchen
 */
public class NodeReader extends MarkLogicRecordReader<NodePath, MarkLogicNode> {

    static final float NODE_TO_FRAGMENT_RATIO = 100;
    
    public NodeReader(Configuration conf) {
        super(conf);
    }

    /**
     * Current key.
     */
    private NodePath currentKey;
    /**
     * Current value.
     */
    private MarkLogicNode currentValue;

    @Override
    public NodePath getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    protected void endOfResult() {
        currentKey = null;
        currentValue = null;
    }

    @Override
    protected boolean nextResult(ResultItem result) {
        String uri = result.getDocumentURI();
        String path = result.getNodePath();
        if (currentKey != null) {
            currentKey.set(uri, path);
        } else {
            currentKey = new NodePath(uri, path);
        }
        if (currentValue != null) {
            currentValue.set(result);
        } else {
            currentValue = new MarkLogicNode(result);
        }
        
        return true;
    }

    @Override
    public MarkLogicNode getCurrentValue() throws IOException,
            InterruptedException {
        return currentValue;
    }

    @Override
    protected float getDefaultRatio() {
        return NODE_TO_FRAGMENT_RATIO;
    }
}
