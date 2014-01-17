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
package com.marklogic.dom;

import org.w3c.dom.DocumentType;
import org.w3c.dom.NamedNodeMap;

import com.marklogic.tree.ExpandedTree;

/**
 * Document Type node is not part of XDM. DocumentTypeImpl is simply a dummy
 * class which does nothing but provide a constructor.
 * 
 * @author ali
 * 
 */
public class DocumentTypeImpl extends NodeImpl implements DocumentType {
    
    DocumentTypeImpl(ExpandedTree tree, int node) {
        super(tree, node);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public NamedNodeMap getEntities() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public NamedNodeMap getNotations() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getPublicId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getSystemId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getInternalSubset() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getNodeName() {
        // TODO Auto-generated method stub
        return null;
    }



}
