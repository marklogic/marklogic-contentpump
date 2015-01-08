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

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.marklogic.mapreduce.DocumentInputFormat;
/**
 * Enum of output types for export.
 * @author ali
 *
 */
@SuppressWarnings("unchecked")
public enum ExportOutputType {
    DOCUMENT {
        @Override
        public Class<? extends Writable> getWritableClass() {
            return BytesWritable.class;
        }

        @Override
        public Class<? extends FileOutputFormat> getOutputFormatClass(
                        CommandLine cmdline) {
            if (Command.isOutputCompressed(cmdline)) {
                return ArchiveOutputFormat.class;
            } else {
                return SingleDocumentOutputFormat.class;
            }
        }

        @Override
        public Class<? extends InputFormat> getInputFormatClass() {
            return DocumentInputFormat.class;
        }
    },
    ARCHIVE {
        @Override
        public Class<? extends Writable> getWritableClass() {
            return BytesWritable.class;
        }

        @Override
        public Class<? extends FileOutputFormat> getOutputFormatClass(
                        CommandLine cmdline) {
            return ArchiveOutputFormat.class;
        }

        @Override
        public Class<? extends InputFormat> getInputFormatClass() {
            return DatabaseContentInputFormat.class;
        }
    };
    public abstract Class<? extends Writable> getWritableClass();
    public abstract Class<? extends FileOutputFormat> getOutputFormatClass(
        CommandLine cmdline);
    public abstract Class<? extends InputFormat> getInputFormatClass();
}
