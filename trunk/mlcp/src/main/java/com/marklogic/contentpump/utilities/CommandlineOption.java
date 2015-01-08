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

/**
 * Command line Option which allows an option to be hidden from help and usage.
 * 
 * @author jchen
 */
public class CommandlineOption extends Option {

    private static final long serialVersionUID = 1L;
    private boolean isHidden;

    public CommandlineOption(Option opt)
            throws IllegalArgumentException {
        super(opt.getOpt(), opt.hasArg(), opt.getDescription());

        this.setLongOpt(opt.getLongOpt());
        this.setRequired(opt.isRequired());
        this.setArgName(opt.getArgName());
        this.setArgs(opt.getArgs());
        this.setOptionalArg(opt.hasOptionalArg());
        this.setType(opt.getType());
        this.setValueSeparator(opt.getValueSeparator());
    }
    
    public boolean isHidden() {
        return isHidden;
    }
    
    public void setHidden(boolean hidden) {
        isHidden = hidden;
    }

}
