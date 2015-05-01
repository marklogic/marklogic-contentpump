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
package com.marklogic.contentpump;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.ContentType;
import com.marklogic.mapreduce.ForestInputFormat;
import com.marklogic.mapreduce.LinkedMapWritable;
import com.marklogic.mapreduce.MarkLogicConstants;
import com.marklogic.mapreduce.utilities.InternalUtilities;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.RequestOptions;
import com.marklogic.xcc.ResultSequence;
import com.marklogic.xcc.Session;
import com.marklogic.xcc.exceptions.RequestException;
import com.marklogic.xcc.exceptions.XccConfigException;

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
        
        @Override
        public void applyConfigOptions(Configuration conf,
            CommandLine cmdline) throws IOException {
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

        @Override
        public void applyConfigOptions(Configuration conf,
            CommandLine cmdline) throws IOException {
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
        
        @Override
        public void applyConfigOptions(Configuration conf,
            CommandLine cmdline) throws IOException {
        }
    },
    ARCHIVE {
        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
                        CommandLine cmdline, Configuration conf) {
            return ArchiveInputFormat.class;
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
        
        @Override
        public void applyConfigOptions(Configuration conf,
            CommandLine cmdline) throws IOException{
        }
    },
    SEQUENCEFILE {

        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
                        CommandLine cmdline, Configuration conf) {
            return SequenceFileInputFormat.class;
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
        
        @Override
        public void applyConfigOptions(Configuration conf,
            CommandLine cmdline) throws IOException {
        }
    },
    RDF {
        private String ROLE_QUERY = 
            "import module namespace hadoop = " +
            "\"http://marklogic.com/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n"+
            "hadoop:get-role-map()";
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
        
        @Override
        public void applyConfigOptions(Configuration conf, CommandLine cmdline)
            throws IOException {
            // try getting a connection
            Session session = null;
            ResultSequence result = null;
            ContentSource cs;
            try {
                cs = InternalUtilities.getOutputContentSource(conf,
                    conf.get(MarkLogicConstants.OUTPUT_HOST));
                session = cs.newSession("Security");
                RequestOptions options = new RequestOptions();
                options.setDefaultXQueryVersion("1.0-ml");
                session.setDefaultRequestOptions(options);
                AdhocQuery query = session.newAdhocQuery(ROLE_QUERY);
                query.setOptions(options);
                result = session.submitRequest(query);
                LinkedMapWritable roleMap = new LinkedMapWritable();
                while (result.hasNext()) {
                    Text key = new Text(result.next().asString());
                    if (!result.hasNext()) {
                        throw new IOException("Invalid role map");
                    }
                    Text value = new Text(result.next().asString());
                    roleMap.put(key, value);
                }

                DefaultStringifier.store(conf, roleMap,
                    MarkLogicConstants.ROLE_MAP);
                if (conf.get(MarkLogicConstants.OUTPUT_DIRECTORY) == null
                    && conf.get(CONF_OUTPUT_URI_PREFIX) == null) {
                    conf.set(CONF_OUTPUT_URI_PREFIX, "/triplestore/");
                }
            } catch (XccConfigException e) {
                throw new IOException(e);
            } catch (RequestException e) {
                throw new IOException(e);
            } finally {
                if (result != null) {
                    result.close();
                }
                if (session != null) {
                    session.close();
                }
            }
        }
    },
    FOREST {
        @Override
        public Class<? extends FileInputFormat> getInputFormatClass(
            CommandLine cmdline, Configuration conf) {
            return ForestInputFormat.class;
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
            return ContentType.MIXED;
        }
        
        @Override
        public void applyConfigOptions(Configuration conf,
            CommandLine cmdline) throws IOException {
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
        } else if (type.equalsIgnoreCase(FOREST.name())) {
            return FOREST;
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
    public <K1, V1, K2, V2> Class<? extends BaseMapper<K1, V1, K2, V2>> 
    getMapperClass(CommandLine cmdline, Configuration conf) {
        return (Class<? extends BaseMapper<K1, V1, K2, V2>>) (Class)
        DocumentMapper.class;
    }
    
    public abstract Class<? extends OutputFormat> getOutputFormatClass(
                    CommandLine cmdline, Configuration conf);
    
    public abstract ContentType getContentType(CommandLine cmdline);    
    
    public abstract void applyConfigOptions(Configuration conf,
        CommandLine cmdline) throws IOException;
    
    public int getMinThreads() {
        return 1;
    }
}
