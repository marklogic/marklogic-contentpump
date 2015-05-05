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
package com.marklogic.mapreduce.examples;

import info.bliki.wiki.model.WikiModel;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.GenericOptionsParser;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import com.marklogic.cpox.SimpleLogger;
import com.marklogic.cpox.Utilities;
import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.xcc.Session;

/**
 * Load wiki documents from HDFS into MarkLogic Server.
 * Used with the configuration file conf/marklogic-wiki.xml.
 */

public class WikiLoader {
    public static class ArticleMapper 
    extends Mapper<Text, Text, DocumentURI, Text> {
        
        private DocumentURI uri = new DocumentURI();
        
        public void map(Text path, Text page, Context context) 
        throws IOException, InterruptedException {
            uri.setUri(path.toString());
            context.write(uri, page);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length < 2) {
            System.err.println("Usage: WikiLoader configFile inputDir");
            System.exit(2);
        }
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
       
        Job job = Job.getInstance(conf, "wiki loader");
        job.setJarByClass(WikiLoader.class);
        job.setInputFormatClass(WikiInputFormat.class);
        job.setMapperClass(ArticleMapper.class);
        job.setMapOutputKeyClass(DocumentURI.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(ContentOutputFormat.class);
        
        ContentInputFormat.setInputPaths(job, new Path(otherArgs[1]));

        conf = job.getConfiguration();
        conf.addResource(otherArgs[0]);
         
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

class WikiInputFormat extends FileInputFormat<Text, Text> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return true;
    }
    
    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException, InterruptedException {
        return new WikiReader();
    }
    
}

class Article {
    String title;
    StringBuilder pageContent;
    
    public Article(String title, StringBuilder pageContent) {
        this.title = title;
        this.pageContent = pageContent;
    }
}

class WikiReader extends RecordReader<Text, Text> {

    static final int BUFFER_SIZE = 65536;
    static final int READ_AHEAD_SIZE = 2048;
    static final String BEGIN_PAGE_TAG = "<page>";
    static final String END_PAGE_TAG = "</page>";
    static final String END_DOC_TAG = "</mediawiki>";
    private Text key = new Text();
    private Text value = new Text();
    private List<Article> articles;
    private int recordCount = 0;
    
    public WikiReader() {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (articles == null || articles.isEmpty()) {
            return 0;
        }
        return recordCount / (float)articles.size();
    }

    @Override
    public void initialize(InputSplit inSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        Path file = ((FileSplit)inSplit).getPath();
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        FSDataInputStream fileIn = fs.open(file);
        byte[] buf = new byte[BUFFER_SIZE];
        long bytesTotal = inSplit.getLength();
        long start = ((FileSplit)inSplit).getStart();
        fileIn.seek(start);
        long bytesRead = 0;
        StringBuilder pages = new StringBuilder();
        int sindex = -1;
        while (true) {
            int length = (int)Math.min(bytesTotal - bytesRead, buf.length);
            int read = fileIn.read(buf, 0, length);
            if (read == -1) {
                System.out.println("Unexpected EOF: bytesTotal=" + bytesTotal +
                        "bytesRead=" + bytesRead);
                break;
            }
            bytesRead += read;  
            String temp = new String(new String(buf, 0, read));
            if (sindex == -1) { // haven't found the start yet    
                sindex = temp.indexOf(BEGIN_PAGE_TAG);
                if (sindex > -1) {
                    pages.append(temp.substring(sindex));
                }
            } else if (bytesRead < bytesTotal) { // haven't completed the split
                pages.append(temp);
            } else { // reached the end of this split
                // look for end
                int eindex = 0;
                if (temp.contains(END_DOC_TAG) || // reached the end of doc
                    temp.endsWith(END_PAGE_TAG)) {
                    eindex = temp.lastIndexOf(END_PAGE_TAG);
                    pages.append(temp.substring(0, 
                        eindex + END_PAGE_TAG.length()));   
                    System.out.println("Found end of doc.");
                } else { // need to read ahead to look for end of page
                    while (true) {
                        read = fileIn.read(buf, 0, READ_AHEAD_SIZE);
                        if (read == -1) { // no more to read
                            System.out.println("Unexpected EOF: bytesTotal=" + bytesTotal +
                                    "bytesRead=" + bytesRead);
                            System.out.println(temp);
                            break;
                        }
                        bytesRead += read;
                        // look for end
                        temp = new String(buf, 0, read);
                        eindex = temp.indexOf(END_PAGE_TAG);
                        if (eindex > -1) {
                            pages.append(temp.substring(0, 
                                    eindex + END_PAGE_TAG.length()));
                            break;
                        } else {
                            pages.append(temp);
                        }
                    }
                }
                break;
            }
        }
        fileIn.close();
        articles = WikiModelProcessor.process(pages);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (articles != null && articles.size() > recordCount) {
            Article article = articles.get(recordCount);
            key.set(article.title);
            value.set(article.pageContent.toString());
            recordCount++;
            return true;
        }
        return false;
    }

    static class  WikiModelProcessor {
        /**
         * 
         */
        private static final String TITLE = "title";

        /**
         * 
         */
        private static final String PAGE = "page";

        private static final String ROOT = "mediawiki";

        private static final String NS_XML = "http://www.w3.org/XML/1998/namespace";
        
        private static final String HEADER = 
            "<mediawiki xmlns=\"http://www.mediawiki.org/xml/export-0.4/\" " +
            "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" " +
            "xsi:schemaLocation=\"http://www.mediawiki.org/xml/export-0.4/" +
            "http://www.mediawiki.org/xml/export-0.4.xsd\" version=\"0.4\" " +
            "xml:lang=\"en\"> \n" +
            "  <siteinfo> \n" +
            "    <sitename>Wikipedia</sitename> \n" +
            "    <base>http://en.wikipedia.org/wiki/Main_Page</base> \n" +
            "    <generator>MediaWiki 1.16alpha-wmf</generator> \n" +
            "    <case>first-letter</case> \n" +
            "    <namespaces> \n" +
            "      <namespace key=\"-2\">Media</namespace> \n" +
            "      <namespace key=\"-1\">Special</namespace> \n" +
            "      <namespace key=\"0\" /> \n" +
            "      <namespace key=\"1\">Talk</namespace> \n" +
            "      <namespace key=\"2\">User</namespace> \n" +
            "      <namespace key=\"3\">User talk</namespace> \n" +
            "      <namespace key=\"4\">Wikipedia</namespace> \n" +
            "      <namespace key=\"5\">Wikipedia talk</namespace> \n" +
            "      <namespace key=\"6\">File</namespace> \n" +
            "      <namespace key=\"7\">File talk</namespace> \n" +
            "      <namespace key=\"8\">MediaWiki</namespace> \n" +
            "      <namespace key=\"9\">MediaWiki talk</namespace> \n" +
            "      <namespace key=\"10\">Template</namespace> \n" +
            "      <namespace key=\"11\">Template talk</namespace> \n" +
            "      <namespace key=\"12\">Help</namespace> \n" +
            "      <namespace key=\"13\">Help talk</namespace> \n" +
            "      <namespace key=\"14\">Category</namespace> \n" +
            "      <namespace key=\"15\">Category talk</namespace> \n" +
            "      <namespace key=\"100\">Portal</namespace> \n" +
            "      <namespace key=\"101\">Portal talk</namespace> \n" +
            "    </namespaces> \n" +
            "  </siteinfo> \n";
        
        private static final String FOOTER = "\n</mediawiki>";

        private static LinkedList<String> path;

        private static StringBuilder article;

        private static String title;

        private static XmlPullParser xpp;

        static SimpleLogger logger = SimpleLogger.getSimpleLogger();

        private static int errors = 0;

        private static int pages = 0;

        private static String namespace;

        private static String language;

        private static XmlPullParserFactory factory;

        private static XmlPullParser parser;
        
        private static Session session;
        
        private static List<Article> articles;

        /**
         * @param args
         * @throws Exception
         */
        public static List<Article> process(StringBuilder input) {
            input.insert(0, HEADER);
            input.append(FOOTER);
            Properties properties = new Properties();
            try {
                factory = XmlPullParserFactory.newInstance(properties
                        .getProperty(XmlPullParserFactory.PROPERTY_NAME), null);
                factory.setNamespaceAware(true);
                xpp = factory.newPullParser();
                xpp.setInput(new StringReader(input.toString()));
    
                // TODO feature isn't supported by xpp3 - look at xpp5?
                // xpp.setFeature(XmlPullParser.FEATURE_DETECT_ENCODING, true);
                // TODO feature isn't supported by xpp3 - look at xpp5?
                // xpp.setFeature(XmlPullParser.FEATURE_PROCESS_DOCDECL, true);
                xpp.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES, true);
    
                logger.configureLogger(new Properties());
    
                process();
            } catch (Exception ex) {
                logger.logException(ex);
            }
            logger.info("finished " + pages + " pages with " + errors
                    + " errors");
            return articles;
        }

        /**
         * @throws IOException
         * @throws XmlPullParserException
         */
        private static void process() throws XmlPullParserException,
        IOException {
            // transform to final output
            int event;
            path = new LinkedList<String>();
            article = null;
            title = null;

            logger.info("starting loop");

            while (true) {
                event = xpp.next();
                switch (event) {
                case XmlPullParser.END_DOCUMENT:
                    processEndDocument();
                    // exit the loop
                    return;
                case XmlPullParser.END_TAG:
                    processEndElement(xpp.getName());
                    break;
                case XmlPullParser.START_TAG:
                    processStartElement(xpp.getName());
                    break;
                case XmlPullParser.TEXT:
                    if (null != article) {
                        String name = path.getLast();
                        if ("comment".equals(name) || "text".equals(name)) {
                            // parse comment elements
                            // parse text elements
                            article.append(parse(xpp.getText()));
                        } else {
                            article
                            .append(Utilities
                                    .escapeXml(xpp.getText()));
                        }
                    }
                    break;
                default:
                    throw new IOException("unexpected event: " + event
                            + " at " + xpp.getPositionDescription());
                }
            }
        }

        /**
         * @param text
         * @return
         * @throws IOException
         */
        private static String parse(String text) throws IOException {
            if (null == text || "".equals(text.trim())) {
                return null;
            }
            // parse wiki markup to xml
            // TODO: this is slow with bliki - might need concurrency
            // use a new object every time, to prevent leaks
            // no doubt this makes it slower...
            String xml = new WikiModel("${image}", "${title}").render(text);

            if (null == xml || "".equals(xml.trim())) {
                return xml;
            }

            // verify xml is well-formed
            try {
                // use this xpp object to check output from the wikimedia parser
                parser = factory.newPullParser();
                parser
                .setInput(new StringReader("<dummy>" + xml
                        + "</dummy>"));
                parser.setFeature(XmlPullParser.FEATURE_PROCESS_NAMESPACES,
                        true);
                int event;
                String temp;
                char[] chars;
                int c;
                while (true) {
                    // with some Japanese text, next() throws
                    // ArrayIndexOutOfBoundsException
                    try {
                        event = parser.next();
                    } catch (ArrayIndexOutOfBoundsException e) {
                        throw new XmlPullParserException(e.getMessage(),
                                parser, null);
                    }
                    switch (event) {
                    case XmlPullParser.END_DOCUMENT:
                        // exit the loop
                        return xml;
                    case XmlPullParser.END_TAG:
                        parser.getName();
                        parser.getNamespace();
                        parser.getText();
                        break;
                    case XmlPullParser.START_TAG:
                        parser.getName();
                        parser.getNamespace();
                        parser.getText();
                        break;
                    case XmlPullParser.TEXT:
                        temp = parser.getText();
                        if (null != temp) {
                            chars = temp.toCharArray();
                            // xpp3 doesn't check codepoint values
                            // check them to avoid XDMP errors
                            for (int i = 0; i < chars.length; i++) {
                                c = chars[i];
                                // #x9 | #xA | #xD
                                // | [#x20-#xD7FF]
                                // | [#xE000-#xFFFD]
                                // | [#x10000-#x10FFFF]
                                // this implementation is abbreviated
                                if (9 == c || 10 == c || 13 == c || c > 31) {
                                    continue;
                                }
                                throw new XmlPullParserException(
                                        "bad codepoint value: " + c, parser,
                                        null);
                            }
                        }
                        break;
                    default:
                        throw new IOException("unexpected event: " + event
                                + " at " + parser.getPositionDescription());
                    }
                }
            } catch (XmlPullParserException e) {
                logger.warning(title + ": " + e.getMessage());
                errors++;
                return Utilities.escapeXml(text);
            }
        }

        /**
         * @param name
         * @throws IOException
         */
        private static void processEndElement(String name) throws IOException {
            // logger.info(name);
            if (!path.getLast().equals(name)) {
                throw new IOException("found " + name + " expected "
                        + path.getLast() + "; " + title + "; " + article);
            }
            path.removeLast();

            if (null == article) {
                return;
            }

            article.append(xpp.getText());

            // look for end of article
            if (!PAGE.equals(name)) {
                return;
            }

            boolean encodeTitle = false;
            URI uri = null;
            if (encodeTitle) {
                // try encoding the entry name
                try {
                    // this form of URI() does escaping nicely
                    uri = new URI(null, title, null);
                } catch (URISyntaxException e) {
                    try {
                        // URI(schema, ssp, fragment) constructor cannot handle
                        // ssp = 2008-11-07T12:23:47.617766-08:00/1
                        // (despite what the javadoc says)...
                        // in this situation, treat the path as the fragment.
                        uri = new URI(null, null, title);
                    } catch (URISyntaxException e1) {
                        throw new IOException(e);
                    }
                }
            }

            // add article to list
            // include the language in the title        
            String path = language + "wiki/"
            + (encodeTitle ? uri.toString() : title);
            if (articles == null) {
                articles = new ArrayList<Article>();
            }
            articles.add(new Article(path, article));
           
            // ready for the next page
            article = null;
        }

        /**
         * @param name
         * @throws IOException
         * @throws XmlPullParserException
         */
        private static void processStartElement(String name)
        throws IOException, XmlPullParserException {
            // logger.info(name);
            path.add(name);
            // look for start of article
            if (ROOT.equals(name)) {
                namespace = xpp.getNamespace();
                language = xpp.getAttributeValue(NS_XML, "lang");
                return;
            }

            if (PAGE.equals(name)) {
                if (null != article) {
                    throw new IOException("article not null at start of page");
                }
                // this is clumsy, but should work ok
                article = new StringBuilder("<"
                        + PAGE
                        // propagate the XML namespace
                        + (null == namespace ? ""
                                : (" xmlns=\"" + namespace + "\""))
                                // propagate the xml:lang attribute
                                + (null == language ? ""
                                        : (" xml:lang=\"" + language + "\""))
                                        // end of the start tag
                                        + ">");
                pages++;
                return;
            }

            if (null != article && !xpp.isEmptyElementTag()) {
                // write empty elements via end-element, only.
                // note that attributes are still ok in this case
                article.append(xpp.getText());
            }

            if (!TITLE.equals(name)) {
                return;
            }

            // create zip entry when we see the title element
            title = xpp.nextText().trim();
            article.append(Utilities.escapeXml(title));
            // this puts us at the end element for title
            processEndElement(name);
        }

        /**
         * @throws IOException
         */
        private static void processEndDocument() throws IOException {
            if (0 != path.size()) {
                throw new IOException("document end before end tag ("
                        + path.size() + ") " + path.getLast() + " "
                        + xpp.getPositionDescription());
            }
            if (null != article) {
                throw new IOException("article not null at end of document: "
                        + title + "; " + article.toString() + "; "
                        + xpp.getPositionDescription());
            }
            if (session != null) {
                session.close();
            }
        }
    }
}
