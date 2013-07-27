/*
 * Copyright 2003-2013 MarkLogic Corporation
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

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.ContentType;

/**
 * Enum of supported input type.
 * 
 * @author jchen
 *
 */
@SuppressWarnings("unchecked")
public enum InputType implements ConfigConstants {
    DOCUMENTS {
        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
            CommandLine cmdline, Configuration conf) {
            if (Command.isInputCompressed(cmdline)) {
                return CompressedDocumentInputFormat.class;
            }
            if (Command.isStreaming(cmdline, conf)) {
                return StreamingDocumentInputFormat.class;    
            }
            return CombineDocumentInputFormat.class;
        }

        @Override
        public <K1, V1, K2, V2> Class<? extends BaseMapper<K1, V1, K2, V2>> 
        getMapperClass(CommandLine cmdline, Configuration conf) {
            return (Class<? extends BaseMapper<K1, V1, K2, V2>>) (Class)
            DocumentMapper.class;
        }

        @Override
        public Class<? extends OutputFormat> getOutputFormatClass(
                        CommandLine cmdline, Configuration conf) {
            if(cmdline.hasOption(TRANSFORM_MODULE)) {
                return TransformOutputFormat.class;
            } else {
                return ContentOutputFormat.class;
            }
        }

        @Override
        public ContentType getContentType(CommandLine cmdline) {
            String type = cmdline.getOptionValue(DOCUMENT_TYPE, 
                    ContentType.MIXED.name());
            return ContentType.forName(type);
        }
    },
    AGGREGATES {
        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
                        CommandLine cmdline, Configuration conf) {
            if (Command.isInputCompressed(cmdline)) {
                return CompressedAggXMLInputFormat.class;
            } else {
                return AggregateXMLInputFormat.class;
            }
        }

        @Override
        public <K1, V1, K2, V2> Class<? extends BaseMapper<K1, V1, K2, V2>> 
        getMapperClass(CommandLine cmdline, Configuration conf) {
            return (Class<? extends BaseMapper<K1, V1, K2, V2>>) (Class)
            DocumentMapper.class;
        }

        @Override
        public Class<? extends OutputFormat> getOutputFormatClass(
                        CommandLine cmdline, Configuration conf) {
            if(cmdline.hasOption(TRANSFORM_MODULE)) {
                return TransformOutputFormat.class;
            } else {
                return ContentOutputFormat.class;
            }
        }

        @Override
        public ContentType getContentType(CommandLine cmdline) {
            return ContentType.XML;   
        }
    },   
    DELIMITED_TEXT {
        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
                        CommandLine cmdline, Configuration conf) {
            if (Command.isInputCompressed(cmdline)) {
                return CompressedDelimitedTextInputFormat.class;
            } else {
                return DelimitedTextInputFormat.class;
            }
        }

        @Override
        public <K1, V1, K2, V2> Class<? extends BaseMapper<K1, V1, K2, V2>> 
        getMapperClass(CommandLine cmdline, Configuration conf) {
            return (Class<? extends BaseMapper<K1, V1, K2, V2>>) (Class)
            DocumentMapper.class;
        }

        @Override
        public Class<? extends OutputFormat> getOutputFormatClass(
                        CommandLine cmdline, Configuration conf) {
            if(cmdline.hasOption(TRANSFORM_MODULE)) {
                return TransformOutputFormat.class;
            } else {
                return ContentOutputFormat.class;
            }
        }

        @Override
        public ContentType getContentType(CommandLine cmdline) {
            return ContentType.XML;   
        }
    },
    ARCHIVE {
        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
                        CommandLine cmdline, Configuration conf) {
            return ArchiveInputFormat.class;
        }

        @Override
        public <K1, V1, K2, V2> Class<? extends BaseMapper<K1, V1, K2, V2>> 
        getMapperClass(CommandLine cmdline, Configuration conf) {
            return (Class<? extends BaseMapper<K1, V1, K2, V2>>) (Class)
            DocumentMapper.class;
        }
        
        public Class<? extends OutputFormat> getOutputFormatClass(
                        CommandLine cmdline, Configuration conf) {
            if(cmdline.hasOption(TRANSFORM_MODULE)) {
                return DatabaseTransformOutputFormat.class;
            } else {
                return DatabaseContentOutputFormat.class;
            }
        }
        
        @Override
        public ContentType getContentType(CommandLine cmdline) {
            return ContentType.XML;   
        }
    },
    SEQUENCEFILE {

        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
                        CommandLine cmdline, Configuration conf) {
            return SequenceFileInputFormat.class;
        }

        @Override
        public <K1, V1, K2, V2> Class<? extends BaseMapper<K1, V1, K2, V2>> 
        getMapperClass(CommandLine cmdline, Configuration conf) {
            return (Class<? extends BaseMapper<K1, V1, K2, V2>>) (Class)
            DocumentMapper.class;
        }

        @Override
        public Class<? extends OutputFormat> getOutputFormatClass(
                        CommandLine cmdline, Configuration conf) {
            if(cmdline.hasOption(TRANSFORM_MODULE)) {
                return TransformOutputFormat.class;
            } else {
                return ContentOutputFormat.class;
            }
        }

        @Override
        public ContentType getContentType(CommandLine cmdline) {
            String type = cmdline.getOptionValue(DOCUMENT_TYPE, 
                    ContentType.XML.name());
            return ContentType.forName(type);
        }
    },
    RDF {
        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
                CommandLine cmdline, Configuration conf) {
            if (Command.isInputCompressed(cmdline)) {
                return CompressedRDFInputFormat.class;
            } else {
                return RDFInputFormat.class;
            }
        }

        @Override
        public <K1, V1, K2, V2> Class<? extends BaseMapper<K1, V1, K2, V2>>
        getMapperClass(CommandLine cmdline, Configuration conf) {
            return (Class<? extends BaseMapper<K1, V1, K2, V2>>) (Class)
                    DocumentMapper.class;
        }

        @Override
        public Class<? extends OutputFormat> getOutputFormatClass(
                CommandLine cmdline, Configuration conf) {
            if(cmdline.hasOption(TRANSFORM_MODULE)) {
                return TransformOutputFormat.class;
            } else {
                return ContentOutputFormat.class;
            }
        }

        @Override
        public ContentType getContentType(CommandLine cmdline) {
            return ContentType.XML;
        }
    };

    public static InputType forName(String type) {
        if (type.equalsIgnoreCase(DOCUMENTS.name())) {
            return DOCUMENTS;
        } else if (type.equalsIgnoreCase(AGGREGATES.name())) { 
            return AGGREGATES;
        } else if (type.equalsIgnoreCase(DELIMITED_TEXT.name())) {
            return DELIMITED_TEXT;
        } else if (type.equalsIgnoreCase(ARCHIVE.name())) {
            return ARCHIVE;
        } else if (type.equalsIgnoreCase(SEQUENCEFILE.name())) {
            return SEQUENCEFILE;
        } else if (type.equalsIgnoreCase(RDF.name())) {
            return RDF;
        } else {
            throw new IllegalArgumentException("Unknown input type: " + type);
        }
    }
    
    /**
     * Get InputFormat class based on content type.
     * 
     * @param contentType content type
     * @return InputFormat class
     */
    public abstract Class<? extends FileInputFormat> getInputFormatClass(
            CommandLine cmdline, Configuration conf);
    
    /**
     * Get Mapper class based on content type.
     * 
     * @param contentType content type
     * @return Mapper class
     */
    public abstract <K1, V1, K2, V2> Class<? extends BaseMapper<K1, V1, K2, V2>> 
    getMapperClass(
            CommandLine cmdline, Configuration conf);
    
    public abstract Class<? extends OutputFormat> getOutputFormatClass(
                    CommandLine cmdline, Configuration conf);
    
    public abstract ContentType getContentType(CommandLine cmdline);
}
