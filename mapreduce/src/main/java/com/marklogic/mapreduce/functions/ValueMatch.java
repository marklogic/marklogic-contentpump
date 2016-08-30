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
package com.marklogic.mapreduce.functions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;


/**
 * Wrapper class for the <code>cts:value-match</code> lexicon
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
 * </p>
 * 
 * @author jchen
 */
public abstract class ValueMatch extends ValueOrWordMatchFunction {

    /**
     * Get the value of the $range-indexes parameter to the lexicon
     * function call, as an array of References. 
     * 
     * @return an array of References.
     */
    public abstract Reference[] getReferences();
    
    @Override
    void appendFunctionName(StringBuilder buf) {
        buf.append("cts:value-match");
    }
    
    @Override
    void appendNamesParams(StringBuilder buf) {
        Reference[] refs = getReferences();
        buf.append("(");
        for (int i = 0; i < refs.length; i++) {
            if (i > 0) {
                buf.append(",");
            }
            refs[i].append(buf);
        }
        buf.append("),\n");
    }
    
    public static void main(String[] args) {
        ValueMatch matchFunc = new ValueMatchFunction();
        Collection<String> nsbindings = new ArrayList<>(args.length);
        nsbindings.addAll(Arrays.asList(args));
        System.out.println(matchFunc.getInputQuery(nsbindings, 1, 1000));
    }
    
    static class ValueMatchFunction extends ValueMatch {
        @Override
        public String getPattern() {
            return "\"?3\"";
        }

        @Override
        public Reference[] getReferences() {
            PathReference pathRef = new MyPathReference();
            return new Reference[] {pathRef};
        }

    }
    
    static class MyPathReference extends PathReference {

        @Override
        public String getPathExpression() {
            return "/my:a[@his:b='B1']/my:c";
        }

    }
}
