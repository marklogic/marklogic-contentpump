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

import org.apache.hadoop.mapreduce.Mapper;
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
public enum InputType {
    DOCUMENTS {
        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
            ContentType contentType, boolean compressed) {
            if (compressed) {
                return CompressedDocumentInputFormat.class;
            } else {
                return CombineDocumentInputFormat.class;
            }
        }

        @Override
        public Class<? extends Mapper> getMapperClass(ContentType contentType) {
            return DocumentMapper.class;
          }

        @Override
        public Class<? extends OutputFormat> getOutputFormatClass(
            ContentType contentType) {
            // TODO Auto-generated method stub
            return ContentOutputFormat.class;
        }
    },
    AGGREGATES {
        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
                ContentType contentType, boolean compressed) {
            if (compressed) {
                return CompressedAggregateXMLInputFormat.class;
            } else {
                return AggregateXMLInputFormat.class;
            }
        }

        @Override
        public Class<? extends Mapper> getMapperClass(
                ContentType contentType) {
            return DocumentMapper.class;
        }

        @Override
        public Class<? extends OutputFormat> getOutputFormatClass(
            ContentType contentType) {
            // TODO Auto-generated method stub
            return ContentOutputFormat.class;
        }
    },   
    DELIMITED_TEXT {
        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
                ContentType contentType, boolean compressed) {
            if (compressed) {
                return CompressedDelimitedTextInputFormat.class;
            } else {
                return DelimitedTextInputFormat.class;
            }
        }

        @Override
        public Class<? extends Mapper> getMapperClass(
                ContentType contentType) {
            return DocumentMapper.class;
        }

        @Override
        public Class<? extends OutputFormat> getOutputFormatClass(
            ContentType contentType) {
            // TODO Auto-generated method stub
            return ContentOutputFormat.class;
        }
    },
    ARCHIVE {
        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
                ContentType contentType, boolean compressed) {
            // TODO Auto-generated method stub
            return ArchiveInputFormat.class;
        }

        @Override
        public Class<? extends Mapper> getMapperClass(
                ContentType contentType) {
            // TODO Auto-generated method stub
            return DocumentMapper.class;
        }
        
        public Class<? extends OutputFormat> getOutputFormatClass(
                ContentType contentType) {
            // TODO Auto-generated method stub
            return ImportArchiveOutputFormat.class;
        }
    },
    SEQUENCEFILE {

        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
                ContentType contentType, boolean compressed) {
            return SequenceFileInputFormat.class;
        }

        @Override
        public Class<? extends Mapper> getMapperClass(
                ContentType contentType) {
            return DocumentMapper.class;
        }

        @Override
        public Class<? extends OutputFormat> getOutputFormatClass(
            ContentType contentType) {
            // TODO Auto-generated method stub
            return ContentOutputFormat.class;
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
            ContentType contentType, boolean compressed);
    
    /**
     * Get Mapper class based on content type.
     * 
     * @param contentType content type
     * @return Mapper class
     */
    public abstract Class<? extends Mapper> getMapperClass(
            ContentType contentType);
    
    public abstract Class<? extends OutputFormat> getOutputFormatClass(
        ContentType contentType);
}
