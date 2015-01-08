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
package com.marklogic.contentpump.utilities;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * Command line options which supports filtering out hidden options.
 * 
 * @author jchen
 */
public class CommandlineOptions extends Options {

    private static final long serialVersionUID = 1L;

    public Options getPublicOptions() {
        Options publicOptions = new Options();
        
        for (Object option : getOptions()) {
            if (option instanceof CommandlineOption && 
                ((CommandlineOption)option).isHidden()) {
                continue;
            }
            publicOptions.addOption((Option)option);
        }
        return publicOptions;
    }
}
