package com.rtdl.sf.piidetection.function;

import java.util.ArrayList;
import java.util.regex.Pattern;

public class PiiDetector {

    static final String[] TARGETS = {
            "\\d{3}-\\d{2}-\\d{4}",                 // SSN
            "[2-9]\\d{2}-\\d{3}-\\d{4}"             // phone
    };
    static final String MASK = "###";
    ArrayList<Pattern> patterns;

    public PiiDetector() {
        patterns = new ArrayList<>();
        for (String s : TARGETS) {
            patterns.add(Pattern.compile(s));
        }
    }

    public String maskPII(String s) {
        for (Pattern p : patterns) {
            s = p.matcher(s).replaceAll(MASK);
        }
        return s;
    }
}
