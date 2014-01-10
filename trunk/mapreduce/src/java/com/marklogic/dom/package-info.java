/*
 * Copyright 2013-2014 MarkLogic Corporation
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
/**
 * This package provides W3C DOM based APIs to the
 * the internal on-disk representation of documents and their
 * contents in the expanded tree cache of a MarkLogic database
 * forest. 
 * 
 * <p>
 * You cannot use these APIs to modify documents.
 * However, you can use the APIs to clone a modifiable copy
 * of a document or a portion of a document.
 * </p><p>
 * The classes in this package are used to support 
 * {@link com.marklogic.mapreduce.ForestInputFormat} and
 * {@link com.marklogic.mapreduce.ForestDocument}.
 * </p>
 */
package com.marklogic.dom;