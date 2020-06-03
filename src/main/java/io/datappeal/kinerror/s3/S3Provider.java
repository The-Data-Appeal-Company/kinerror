package io.datappeal.kinerror.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.google.gson.Gson;
import io.datappeal.kinerror.model.Error;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.stream.Collectors;

public class S3Provider {

    private final AmazonS3 s3Client;
    private final Gson gson = new Gson();


    public S3Provider(final AmazonS3 s3Client) {
        this.s3Client = s3Client;
    }


    public List<String> listErrorsKeys(String bucket, String prefix) {

        final ListObjectsV2Result objectListing = s3Client.listObjectsV2(bucket, prefix);
        return objectListing.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());

    }

    public Error getError(String bucket, String key) throws IOException {

        S3Object s3Object = s3Client.getObject(bucket, key);
        return gson.fromJson(getObjectAsString(s3Object.getObjectContent()), Error.class);

    }

    public List<String> getData(String bucket, String key) throws IOException {
        try {
            final S3Object s3Object = s3Client.getObject(bucket, key);

            String[] split = getObjectAsString(s3Object.getObjectContent()).split("\\}\\{");
            return Arrays.stream(split).map(v -> "{" + v.replaceFirst("\\{", "").replaceFirst("\\}", "") + "}").collect(Collectors.toList());

        } catch (AmazonS3Exception e) {
            return Collections.emptyList();
        }
    }

    public void removeKey(String bucket, String key) {
        s3Client.deleteObject(bucket, key);
        System.out.println(String.format("Removed %s/%s", bucket, key));
    }


    private String getObjectAsString(InputStream input) throws IOException {
        StringJoiner sj = new StringJoiner("");
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line;
        while ((line = reader.readLine()) != null) {
            sj.add(line);
        }
        return sj.toString();
    }


}
