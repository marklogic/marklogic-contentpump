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
package com.marklogic.contentpump.utilities;

import javax.xml.namespace.NamespaceContext;
import javax.xml.stream.XMLStreamException;

import com.sun.org.apache.xerces.internal.impl.PropertyManager;
import com.sun.org.apache.xerces.internal.impl.XMLStreamReaderImpl;
import com.sun.org.apache.xerces.internal.util.NamespaceContextWrapper;
import com.sun.org.apache.xerces.internal.util.NamespaceSupport;
import com.sun.org.apache.xerces.internal.xni.parser.XMLInputSource;

public class AggregateXMLStreamReaderImpl extends XMLStreamReaderImpl {


    
    public AggregateXMLStreamReaderImpl(XMLInputSource inputSource,
        PropertyManager props) throws XMLStreamException {
        super(inputSource, props);
    }

    /**
     * overwrite getNamespaceContext() so that it returns the latest context
     */
    @Override
    public NamespaceContext getNamespaceContext() {
        return new NamespaceContextWrapper((NamespaceSupport)fScanner.getNamespaceContext()) ;
    }

}
