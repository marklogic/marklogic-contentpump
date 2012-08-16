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

import java.io.InputStream;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import com.sun.org.apache.xerces.internal.impl.PropertyManager;
import com.sun.org.apache.xerces.internal.impl.XMLStreamReaderImpl;
import com.sun.org.apache.xerces.internal.xni.parser.XMLInputSource;
import com.sun.xml.internal.stream.XMLInputFactoryImpl;

/**
 * XMLInputFactory for creating AggregateXMLStreamReaderImpl
 * 
 * @author ali
 */
@SuppressWarnings("all")
public class AggregateXMLInputFactoryImpl extends XMLInputFactoryImpl {
    //List of supported properties and default values.
    private PropertyManager fPropertyManager = new PropertyManager(PropertyManager.CONTEXT_READER) ;
    private static final boolean DEBUG = false;
    
    //Maintain a reference to last reader instantiated.
    private XMLStreamReaderImpl fTempReader = null ;
    
    boolean fPropertyChanged = false;
    //no reader reuse by default
    boolean fReuseInstance = false;
    public XMLStreamReader createXMLStreamReader(InputStream inputstream) 
    throws XMLStreamException {
        XMLInputSource inputSource = new XMLInputSource(null, null, null, inputstream, null);
        return getXMLStreamReaderImpl(inputSource);
    }

    XMLStreamReader getXMLStreamReaderImpl(XMLInputSource inputSource) 
    throws javax.xml.stream.XMLStreamException {
        //1. if the temp reader is null -- create the instance and return
        if(fTempReader == null){
            fPropertyChanged = false;
            return fTempReader = new AggregateXMLStreamReaderImpl(inputSource, 
                    new PropertyManager(fPropertyManager));
        }
        // If factory is configured to reuse the instance & this instance can 
        // be reused and the setProperty() hasn't been called
        if (fReuseInstance && fTempReader.canReuse() && !fPropertyChanged){
            if(DEBUG)System.out.println("Reusing the instance");
            // We can make setInputSource() call reset() and this way there 
            // won't be two function calls.
            fTempReader.reset();
            fTempReader.setInputSource(inputSource);
            fPropertyChanged = false;
            return fTempReader;
        } else {
            fPropertyChanged = false;
            // Just return the new instance.. note that we are not setting  
            // fTempReader to the newly created instance
            return fTempReader = new AggregateXMLStreamReaderImpl(inputSource, 
                    new PropertyManager(fPropertyManager));
        }
    }
}
