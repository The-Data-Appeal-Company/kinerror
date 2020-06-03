package io.datappeal.kinerror.model;

import java.util.List;

public class Error {

    private final List<Entry> entries;


    public Error(List<Entry> entries) {
        this.entries = entries;
    }

    public List<Entry> getEntries() {
        return entries;
    }
}
