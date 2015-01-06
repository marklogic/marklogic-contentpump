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
 * Wrapper class for the <code>cts:field-words</code> lexicon
 * function. Subclass this class to generate map input based on a lexicon.
 * 
 * <p>
 *   Use this class with {@link com.marklogic.mapreduce.ValueInputFormat}.
 * </p>
 * <p>
 *   To generate map input using the lexicon function,
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
 * @author jchen
 */
public abstract class FieldWords extends ValuesOrWordsFunction {

    /**
     * Get the value of the $field-names parameter to the lexicon
     * function call, as an array of strings. Each string should be
     * a field name.
     * 
     * @return an array of element QNames.
     */
    public abstract String[] getFieldNames();
    
    @Override
    void appendFunctionName(StringBuilder buf) {
        buf.append("cts:field-words");
    }
    
    @Override
    void appendNamesParams(StringBuilder buf) {
        String[] elemNames = getFieldNames();
        buf.append("(");
        for (int i = 0; i < elemNames.length; i++) {
            if (i > 0) {
                buf.append(",");
            }
            buf.append(elemNames[i]);
        }
        buf.append("),\n");
    }
}
