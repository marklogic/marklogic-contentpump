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
package com.marklogic.contentpump;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.ContentWriter;

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
            CommandLine cmdline) {
            if (Command.isStreaming(cmdline)) {
                return StreamingDocumentInputFormat.class;    
            }
            if (Command.isInputCompressed(cmdline)) {
                return CompressedDocumentInputFormat.class;
            }
            return CombineDocumentInputFormat.class;
        }

        @Override
        public Class<? extends Mapper> getMapperClass(CommandLine cmdline) {
            return DocumentMapper.class;
        }

        @Override
        public Class<? extends OutputFormat> getOutputFormatClass(
                        CommandLine cmdline) {
            return ContentOutputFormat.class;
        }

        @Override
        public Class<? extends RecordWriter> getOutputValueClass(
                        CommandLine cmdline) {
            return ContentWriter.class;
        }
    },
    AGGREGATES {
        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
                        CommandLine cmdline) {
            if (Command.isInputCompressed(cmdline)) {
                return CompressedAggXMLAdvInputFormat.class;
            } else {
                return AggregateXMLAdvInputFormat.class;
            }
        }

        @Override
        public Class<? extends Mapper> getMapperClass(
                        CommandLine cmdline) {
            return DocumentMapper.class;
        }

        @Override
        public Class<? extends OutputFormat> getOutputFormatClass(
                        CommandLine cmdline) {
            return ContentOutputFormat.class;
        }

        @Override
        public Class<? extends RecordWriter> getOutputValueClass(
                        CommandLine cmdline) {
            return ContentWriter.class;
        }
    },   
    DELIMITED_TEXT {
        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
                        CommandLine cmdline) {
            if (Command.isInputCompressed(cmdline)) {
                return CompressedDelimitedTextInputFormat.class;
            } else {
                return DelimitedTextInputFormat.class;
            }
        }

        @Override
        public Class<? extends Mapper> getMapperClass(
                        CommandLine cmdline) {
            return DocumentMapper.class;
        }

        @Override
        public Class<? extends OutputFormat> getOutputFormatClass(
                        CommandLine cmdline) {
            return ContentOutputFormat.class;
        }

        @Override
        public Class<? extends RecordWriter> getOutputValueClass(
                        CommandLine cmdline) {
            return ContentWriter.class;
        }
    },
    ARCHIVE {
        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
                        CommandLine cmdline) {
            return ArchiveInputFormat.class;
        }

        @Override
        public Class<? extends Mapper> getMapperClass(
                        CommandLine cmdline) {
            return DocumentMapper.class;
        }
        
        public Class<? extends OutputFormat> getOutputFormatClass(
                        CommandLine cmdline) {
            return ImportArchiveOutputFormat.class;
        }

        @Override
        public Class<? extends RecordWriter> getOutputValueClass(
                        CommandLine cmdline) {
            return MarkLogicDocumentContentWriter.class;
        }
    },
    SEQUENCEFILE {

        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
                        CommandLine cmdline) {
            return SequenceFileInputFormat.class;
        }

        @Override
        public Class<? extends Mapper> getMapperClass(
                        CommandLine cmdline) {
            return DocumentMapper.class;
        }

        @Override
        public Class<? extends OutputFormat> getOutputFormatClass(
                        CommandLine cmdline) {
            return ContentOutputFormat.class;
        }

        @Override
        public Class<? extends RecordWriter> getOutputValueClass(
                        CommandLine cmdline) {
            return ContentWriter.class;
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
            CommandLine cmdline);
    
    /**
     * Get Mapper class based on content type.
     * 
     * @param contentType content type
     * @return Mapper class
     */
    public abstract Class<? extends Mapper> getMapperClass(CommandLine cmdline);
    
    public abstract Class<? extends OutputFormat> getOutputFormatClass(
                    CommandLine cmdline);
    
    public abstract Class<? extends RecordWriter> getOutputValueClass(
        CommandLine cmdline);
}
