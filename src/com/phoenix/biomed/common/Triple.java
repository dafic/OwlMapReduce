package com.phoenix.biomed.common;

/**
 *
 * @author phoenix
 */
public class Triple {

    String subject;
    String predicate;
    String object;

    public Triple(String triple) {
        String[] split = triple.split(",");
        this.subject = split[0].trim();
        this.predicate = split[1].trim();
        this.object = split[2].trim();
    }

    public String getSubject() {
        return subject;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public String getPredicate() {
        return predicate;
    }

    public void setPredicate(String predicate) {
        this.predicate = predicate;
    }

    public String getObject() {
        return object;
    }

    public void setObject(String object) {
        this.object = object;
    }
}
