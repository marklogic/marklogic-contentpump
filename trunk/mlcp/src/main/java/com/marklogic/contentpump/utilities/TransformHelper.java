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

package com.marklogic.contentpump.utilities;

import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import com.marklogic.contentpump.QueriedDocumentWithMeta;
import com.marklogic.contentpump.RDFWritable;
import com.marklogic.contentpump.TransformOutputFormat;
import com.marklogic.io.Base64;
import com.marklogic.mapreduce.ContentType;
import com.marklogic.xcc.AdhocQuery;
import com.marklogic.xcc.ContentCapability;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentPermission;
import com.marklogic.xcc.DocumentRepairLevel;
import com.marklogic.xcc.types.ValueType;

/**
 * Helper class for server-side transform
 * @author ali
 *
 */
public class TransformHelper {
    public static final Log LOG = LogFactory.getLog(TransformHelper.class);
    private static String MAP_ELEM_START_TAG = 
        "<map:map xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi"
        + "=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:map=\"http:"
        + "//marklogic.com/xdmp/map\">";

    private static String getInvokeModuleQuery(String moduleUri, String functionNs, String functionName,
        String functionParam) {
        StringBuilder q = new StringBuilder();
        q.append("xquery version \"1.0-ml\";\n")
            .append("import module namespace hadoop = \"http://marklogic.com")
            .append("/xdmp/hadoop\" at \"/MarkLogic/hadoop.xqy\";\n")
            .append("declare variable $URI as xs:string external;\n")
            .append("declare variable $CONTENT as item() external;\n")
            .append(
                "declare variable $INSERT-OPTIONS as element() external;\n")
            .append("hadoop:transform-and-insert(\"").append(moduleUri)
            .append("\",\"").append(functionNs).append("\",\"")
            .append(functionName).append("\",\"").append(functionParam)
            .append("\", $URI, $CONTENT, $INSERT-OPTIONS)");
        return q.toString();
    }

    private static String getTypeFromMap(String uri) {
        int idx = uri.lastIndexOf(".");
        Text format = null;
        if (idx != -1) {
            String suff = uri.substring(idx + 1, uri.length());
            if (suff.equalsIgnoreCase("xml"))
                return "xml";
            format = (Text) TransformOutputFormat.mimetypeMap.get(suff);
        }
        if (format == null) {
            return "binary";
        } else {
            return format.toString();
        }
    }

    public static String constructQryString(String moduleUri,
        String functionNs, String functionName, String functionParam) {
        String q = getInvokeModuleQuery(moduleUri, functionNs, functionName,
                    functionParam);
        if (LOG.isDebugEnabled()) {
            LOG.debug(q);
        }
        return q;
    }
    /**
     * for Import all file types except archive.
     *  
     * @param conf
     * @param query
     * @param moduleUri
     * @param functionNs
     * @param functionName
     * @param functionParam
     * @param uri
     * @param value
     * @param type
     * @param cOptions
     * @return
     * @throws InterruptedIOException
     * @throws UnsupportedEncodingException
     */
    public static AdhocQuery getTransformInsertQry(Configuration conf,
        AdhocQuery query, String moduleUri, String functionNs,
        String functionName, String functionParam, String uri,
        Object value, String type, ContentCreateOptions cOptions)
        throws InterruptedIOException, UnsupportedEncodingException {
        HashMap<String, String> optionsMap = new HashMap<String, String>();

        query.setNewStringVariable("URI", uri);
        ContentType contentType = ContentType.valueOf(type);
        if (contentType == ContentType.MIXED) {
            // get type from mimetype map
            contentType = ContentType.forName(getTypeFromMap(uri));
        }

        switch (contentType) {
        case BINARY:
            query.setNewVariable("CONTENT", ValueType.XS_BASE64_BINARY, Base64
                .encodeBytes(((BytesWritable) value).getBytes(), 0,
                    ((BytesWritable) value).getLength()));
            optionsMap
                .put("value-type", ValueType.XS_BASE64_BINARY.toString());
            break;
        case TEXT:
            if (value instanceof BytesWritable) {
                // in MIXED type, value is byteswritable
                String encoding = cOptions.getEncoding();
                query.setNewStringVariable("CONTENT", new String(
                    ((BytesWritable) value).getBytes(), 0,
                    ((BytesWritable) value).getLength(), encoding));
            } else {
                // must be text or xml
                query.setNewStringVariable("CONTENT",
                    ((Text) value).toString());
            }
            optionsMap.put("value-type", ValueType.TEXT.toString());
            break;
        case XML:
            if (value instanceof BytesWritable) {
                // in MIXED type, value is byteswritable
                String encoding = cOptions.getEncoding();
                query.setNewStringVariable("CONTENT", new String(
                    ((BytesWritable) value).getBytes(), 0,
                    ((BytesWritable) value).getLength(), encoding));
            } else if (value instanceof RDFWritable) {
                //RDFWritable's value is Text
                query.setNewStringVariable("CONTENT",
                    ((RDFWritable) value).getValue().toString());
            } else {
                // must be text or xml
                query.setNewStringVariable("CONTENT",
                    ((Text) value).toString());
            }
            optionsMap.put("value-type", ValueType.XS_STRING.toString());
            break;
        case MIXED:
        case UNKNOWN:
            throw new InterruptedIOException("Unexpected:" + contentType);
        default:
            throw new UnsupportedOperationException("invalid type:"
                + contentType);
        }
        String namespace = cOptions.getNamespace();
        if (namespace != null) {
            optionsMap.put("namespace", namespace);
        }
        String lang = cOptions.getLanguage();
        if (lang != null) {
            optionsMap.put("language", "default-language=" + lang);
        }
        ContentPermission[] perms = cOptions.getPermissions();
        if (perms != null && perms.length > 0) {
            for (ContentPermission cp : perms) {
                String roleName = cp.getRole();
                if (roleName == null || roleName.isEmpty()) {
                    LOG.error("Illegal role name: " + roleName);
                    continue;
                }
                ContentCapability cc = cp.getCapability();
                if (cc.equals(ContentCapability.READ)) {
                    optionsMap.put("roles-read", roleName);
                } else if (cc.equals(ContentCapability.EXECUTE)) {
                    optionsMap.put("roles-execute", roleName);
                } else if (cc.equals(ContentCapability.INSERT)) {
                    optionsMap.put("roles-insert", roleName);
                } else if (cc.equals(ContentCapability.UPDATE)) {
                    optionsMap.put("roles-update", roleName);
                }
            }
        }

        String[] collections = cOptions.getCollections();
        if (collections != null) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < collections.length; i++) {
                if (i != 0)
                    sb.append(",");
                sb.append(collections[i].trim());
            }
            optionsMap.put("collections", sb.toString());
        }

        optionsMap.put("quality", String.valueOf(cOptions.getQuality()));
        DocumentRepairLevel repairLevel = cOptions.getRepairLevel();
        if (!DocumentRepairLevel.DEFAULT.equals(repairLevel)) {
            optionsMap.put("xml-repair-level", "repair-" + repairLevel);
        }

        String optionElem = mapToElement(optionsMap);
        query.setNewVariable("INSERT-OPTIONS", ValueType.ELEMENT, optionElem);
        return query;
    }

    /**
     * Get transform and insert query for MarkLogicDocumentWithMeta, 
     * used in importing archive, copy
     * @param conf
     * @param session
     * @param moduleUri
     * @param functionNs
     * @param functionName
     * @param functionParam
     * @param uri
     * @param doc
     * @param cOptions
     * @return
     * @throws InterruptedIOException
     * @throws UnsupportedEncodingException
     */
    public static AdhocQuery getTransformInsertQryMLDocWithMeta(
        Configuration conf, AdhocQuery query, String moduleUri,
        String functionNs, String functionName, String functionParam,
        String uri, QueriedDocumentWithMeta doc,
        ContentCreateOptions cOptions) throws InterruptedIOException,
        UnsupportedEncodingException {
        HashMap<String, String> optionsMap = new HashMap<String, String>();

        query.setNewStringVariable("URI", uri);
        ContentType contentType = doc.getContentType();
        switch (contentType) {
        case BINARY:
            query.setNewVariable("CONTENT", ValueType.XS_BASE64_BINARY,
                Base64.encodeBytes(doc.getContentAsByteArray()));
            optionsMap
                .put("value-type", ValueType.XS_BASE64_BINARY.toString());
            break;
        case TEXT:
            query.setNewStringVariable("CONTENT", doc.getContentAsText()
                .toString());

            optionsMap.put("value-type", ValueType.TEXT.toString());
            break;
        case XML:
            query.setNewStringVariable("CONTENT", doc.getContentAsText()
                .toString());

            optionsMap.put("value-type", ValueType.XS_STRING.toString());
            break;
        default:
            throw new UnsupportedOperationException("invalid type:"
                + contentType);
        }
        String namespace = cOptions.getNamespace();
        if (namespace != null) {
            optionsMap.put("namespace", namespace);
        }
        String lang = cOptions.getLanguage();
        if (lang != null) {
            optionsMap.put("language", "default-language=" + lang);
        }
        ContentPermission[] perms = cOptions.getPermissions();
        if (perms != null && perms.length > 0) {
            for (ContentPermission cp : perms) {
                String roleName = cp.getRole();
                if (roleName == null || roleName.isEmpty()) {
                    LOG.error("Illegal role name: " + roleName);
                    continue;
                }
                ContentCapability cc = cp.getCapability();
                if (cc.equals(ContentCapability.READ)) {
                    optionsMap.put("roles-read", roleName);
                } else if (cc.equals(ContentCapability.EXECUTE)) {
                    optionsMap.put("roles-execute", roleName);
                } else if (cc.equals(ContentCapability.INSERT)) {
                    optionsMap.put("roles-insert", roleName);
                } else if (cc.equals(ContentCapability.UPDATE)) {
                    optionsMap.put("roles-update", roleName);
                }
            }
        }

        String[] collections = cOptions.getCollections();
        if (collections != null) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < collections.length; i++) {
                if (i != 0)
                    sb.append(",");
                sb.append(collections[i].trim());
            }
            optionsMap.put("collections", sb.toString());
        }

        optionsMap.put("quality", String.valueOf(cOptions.getQuality()));
        DocumentRepairLevel repairLevel = cOptions.getRepairLevel();
        if (!DocumentRepairLevel.DEFAULT.equals(repairLevel)) {
            optionsMap.put("xml-repair-level", "repair-" + repairLevel);
        }

        String optionElem = mapToElement(optionsMap);
        query.setNewVariable("INSERT-OPTIONS", ValueType.ELEMENT, optionElem);
        return query;
    }

    private static String mapToElement(HashMap<String, String> map) {
        StringBuilder sb = new StringBuilder();
        sb.append(MAP_ELEM_START_TAG);
        Set<String> keys = map.keySet();
        for (String k : keys) {
            addKeyValue(sb, k, map.get(k));
        }
        sb.append("</map:map>");
        return sb.toString();
    }

    private static void addKeyValue(StringBuilder sb, String key, String value) {
        sb.append("<map:entry key=\"").append(key)
            .append("\"><map:value xsi:type=\"xs:string\">").append(value)
            .append("</map:value></map:entry>");
    }
}
