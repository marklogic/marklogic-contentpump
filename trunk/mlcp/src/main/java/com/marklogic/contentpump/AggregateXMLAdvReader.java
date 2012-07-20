/*
 * Copyright 2003-2012 MarkLogic Corporation
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

import javax.xml.stream.XMLStreamException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.sun.org.apache.xerces.internal.xni.NamespaceContext;

public class AggregateXMLAdvReader extends AggregateXMLReader<Text> {

    public AggregateXMLAdvReader(String recordElem, String namespace, NamespaceContext nsctx) {
        recordName = recordElem;
        recordNamespace = namespace;
        this.nsctx = nsctx;
    }

    @Override
    public void initialize(InputSplit is, TaskAttemptContext context)
        throws IOException, InterruptedException {
        AggregateSplit asplit = (AggregateSplit) is;
        initAggConf(asplit, context);
        nameSpaces = asplit.getNamespaces();
        recordName = asplit.getRecordElem();
        nsctx = asplit.getNamespaceContext();
        conf = context.getConfiguration();
        file = asplit.getPath();
        start = asplit.getStart();
        end = start + asplit.getLength();
        initCommonConfigurations(conf, file);
        configFileNameAsCollection(conf, file);
        fs = file.getFileSystem(context.getConfiguration());
        fInputStream = fs.open(file);
        f = new AggregateXMLInputFactoryImpl();
        long fileLen = fs.getFileStatus(file).getLen();
        
        if (asplit.getStart() == 0 && fileLen == asplit.getLength()) {
            //this split contains the whole file
            parallelMode = false;
            try {
                xmlSR = f.createXMLStreamReader(fInputStream);
            } catch (XMLStreamException e) {
                e.printStackTrace();
            }
        } else {
            //jump to the first record elem in the split
            moveToNextRecord(start);
            parallelMode = true;
        }
//        System.out.println("namespace context: " + nsctx +":" +  nsctx.getDeclaredPrefixCount());
    }

}