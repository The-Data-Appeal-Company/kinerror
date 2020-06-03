package io.datappeal.kinerror.model;

public class Entry {

    private final String url;
    private final Boolean mandatory;

    public Entry(String url, Boolean mandatory) {
        this.url = url;
        this.mandatory = mandatory;
    }

    public String getUrl() {
        return url;
    }

    public Boolean getMandatory() {
        return mandatory;
    }
}
