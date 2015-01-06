/*
 * Copyright 2003-2015 MarkLogic Corporation
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
package com.marklogic.mapreduce.functions;

/**
 * Wrapper class for the <code>cts:element-value-co-occurrences</code>
 * lexicon function. Subclass this class to generate map input based on a 
 * lexicon.
 * 
 * <p>
 *   Use this class with {@link com.marklogic.mapreduce.KeyValueInputFormat}
 *   or {@link com.marklogic.mapreduce.ValueInputFormat}.
 * </p>
 * <p>
 *   To generate map input from the lexicon function,
 *   create a subclass of this class and provide implementations
 *   of the methods that correspond to the function parameters you
 *   want to include in the call.
 * </p>
 * <p>
 *   For details, see "Using a Lexicon to Generate Key-Value Pairs"
 *   in the <em>MarkLogic Connector for Hadoop Developer's Guide</em>.
 *   For an example, see
 *   {@link com.marklogic.mapreduce.examples.LinkCountCooccurrences}.

 * </p>
 * 
 * <p>
 *   All co-occurrences functions using this API do NOT preserve 
 *   frequency order even if "frequency-order" option is specified.
 * </p>
 * 
 * @author jchen
 */
public abstract class ElemValueCooccurrences extends CooccurrencesFunction {

    /**
     * Get the value for the $element-name-1 parameter to the
     * lexicon function call, as a string. The returned string
     * must evaluate to an xs:QName when evaluated as XQuery.
     * For example: "xs:QName(\"wp:a\")".
     * 
     * @return first element QName.
     */
    public abstract String getElementName1();
    
    /**
     * Get the value for the $element-name-2 parameter to the
     * lexicon function call, as a string. The returned string
     * must evaluate to an xs:QName when evaluated as XQuery.
     * For example: "xs:QName(\"wp:a\")".
     * 
     * @return first element QName.
     */
    public abstract String getElementName2();
    
    @Override
    void appendFunctionName(StringBuilder buf) {
        buf.append("cts:element-value-co-occurrences");
    }
    
    @Override
    void appendNamesParams(StringBuilder buf) {
        // first element name   
        buf.append(getElementName1()).append(",\n");
        // second element name
        buf.append(getElementName2()).append(",\n");;
    }
}
