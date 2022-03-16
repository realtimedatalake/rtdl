package com.rtdl.sf.piidetection.function;

import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * PII detector class that includes the algorithm which identifies SSN and phone number patterns in the value
 * and applies masking where PII values are replaced with ###.
 */
public class PiiDetector {

    /**
     * Regex for SSN and phone number.
     */
    static final String[] TARGETS = {
            "\\d{3}-\\d{2}-\\d{4}", // SSN
            "[2-9]\\d{2}-\\d{3}-\\d{4}" // phone
    };

    static final String MASK = "###";
    ArrayList<Pattern> patterns;

    /**
     * Instantiates a new Pii detector. Registers TARGET value to patters list.
     */
    public PiiDetector() {
        patterns = new ArrayList<>();
        for (String s : TARGETS) {
            patterns.add(Pattern.compile(s));
        }
    }

    /**
     * Masks pii string. Replaces matched input with ###.
     *
     * @param inputString the input string
     * @return the masked string
     */
    public String maskPII(String inputString) {
        for (Pattern p : patterns) {
            inputString = p.matcher(inputString).replaceAll(MASK);
        }
        return inputString;
    }
}
