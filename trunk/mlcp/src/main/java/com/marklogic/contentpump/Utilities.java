package com.marklogic.contentpump;

import java.util.regex.Pattern;

public class Utilities {
    protected static Pattern[] patterns = new Pattern[] {
            Pattern.compile("&"), Pattern.compile("<"), Pattern.compile(">") };

    public static String escapeXml(String _in) {
        if (null == _in){
            return "";
        }
        return patterns[2].matcher(
                patterns[1].matcher(
                        patterns[0].matcher(_in).replaceAll("&amp;"))
                        .replaceAll("&lt;")).replaceAll("&gt;");
    }
    
    public static String stringArrayToCommaSeparatedString(String [] arrayOfString) {
        StringBuilder result = new StringBuilder();
        for(String string : arrayOfString) {
            result.append(string);
            result.append(",");
        }
        return result.substring(0, result.length() - 1) ;
    }

}
