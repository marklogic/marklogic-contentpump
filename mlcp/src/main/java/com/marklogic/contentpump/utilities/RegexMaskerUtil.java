package com.marklogic.contentpump.utilities;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexMaskerUtil {

    public static String maskData(String buffer) {
        String debugMsg = buffer;
        for (RegexMasker maskString : RegexMasker.values()) {
            StringBuffer result = new StringBuffer();
            Pattern pattern = Pattern.compile(maskString.getRegEx());
            Matcher matcher = pattern.matcher(debugMsg);
            while (matcher.find()) {
                matcher.appendReplacement(result, matcher.group(1) + "***masked***");
            }
            matcher.appendTail(result);
            debugMsg=result.toString();
        }
        return debugMsg;
    }

    public enum RegexMasker {
        PASSWORD("(-password\\s)(\\S+)"),
        INPUT_PASSWORD("(-input_password\\s)(\\S+)"),
        OUTPUT_PASSWORD("(-output_password\\s)(\\S+)"),
        KEYSTORE_PASSWORD("(-keystore_passwd\\s)(\\S+)"),
        TRUSTSTORE_PASSWORD("(-truststore_passwd\\s)(\\S+)");

        private String regEx;

        RegexMasker(String exp) {
            regEx = exp;
        }
        public String getRegEx() {
            return regEx;
        }
    }
}
