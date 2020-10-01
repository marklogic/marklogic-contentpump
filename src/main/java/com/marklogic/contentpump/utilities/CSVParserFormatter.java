/*
 * Copyright (c) 2020 MarkLogic Corporation
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

import org.apache.commons.csv.CSVFormat;

/**
 * Format the CSVParser with given parameters.
 * @author mattsun
 *
 */
public class CSVParserFormatter {
	
	/**
	 * 
	 * @param delimiter
	 * @param encapsulator
	 * @param ignoreSurroundingSpaces
	 * @param ignoreEmptyLines
	 * @return
	 */
	public static CSVFormat getFormat(char delimiter,
			Character encapsulator,
			boolean ignoreSurroundingSpaces, 
			boolean ignoreEmptyLines) {
		CSVFormat format = CSVFormat.newFormat(delimiter);
		format = format.withIgnoreEmptyLines(ignoreEmptyLines)
        		.withIgnoreSurroundingSpaces(ignoreSurroundingSpaces)
        		.withAllowMissingColumnNames(true)
        		.withQuote(encapsulator);
		
		return format;
	}
}
