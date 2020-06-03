package io.datappeal.kinerror;

import java.io.InputStream;

public class TestResources {

    public static InputStream getResource(String path){
        return TestResources.class.getResourceAsStream("/" + path);
    }
}
