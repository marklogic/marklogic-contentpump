/*
 * Copyright (c) 2019 MarkLogic Corporation
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

import java.text.NumberFormat;
import java.text.ParseException;

/**
 * Enum of column data types of delimited text.
 * 
 * @author mattsun
 *
 */
public enum ColumnDataType {
    STRING {
        @Override
        public Object parse(String s) throws Exception {
            return new String(s);
        }
        
    },
    NUMBER {
        @Override
        public Object parse(String s) throws Exception {
            s.trim();
            if ("".equals(s)) {
                throw new Exception("missing value");
            }
            return NumberFormat.getInstance().parse(s);
        }
        
    },
    BOOLEAN {
        @Override
        public Object parse(String s) throws Exception {
            s.trim();
            if ("".equals(s)) {
                throw new Exception("missing value");
            } else if (!"true".equalsIgnoreCase(s) && 
                    !"false".equalsIgnoreCase(s)) {
                throw new ParseException("", 0);
            }
            return new Boolean(s);
        }
        
    };
    
    /**
     * Parse the string as the specified data type.
     * @param s
     * @return
     * @throws Exception
     */
    public abstract Object parse(String s) throws Exception;
}
