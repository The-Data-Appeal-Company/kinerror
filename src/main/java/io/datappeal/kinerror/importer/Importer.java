package io.datappeal.kinerror.importer;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.Record;
import io.datappeal.kinerror.model.Entry;
import io.datappeal.kinerror.model.Error;
import io.datappeal.kinerror.s3.S3Provider;
import org.apache.commons.collections4.ListUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Importer {

    private final S3Provider s3Provider;
    final AmazonKinesisFirehose amazonKinesisFirehose;
    private final String deliveryStreamName;
    private final ExecutorService executor;

    final String regex = "s3:\\/\\/(.*?)\\/(.*)";


    public Importer(final S3Provider s3Provider,
                    final AmazonKinesisFirehose kinesisFirehose,
                    final String deliveryStreamName, int concurrency) {
        this.s3Provider = s3Provider;
        this.amazonKinesisFirehose = kinesisFirehose;
        this.deliveryStreamName = deliveryStreamName;

        this.executor = Executors.newFixedThreadPool(concurrency);
    }


    /**
     * Process the part of bucket with error files,
     *
     * @param bucketErr the name of the s3 bucket
     * @param prefix    the key of the s3 document error to process
     */
    public void process(final String bucketErr, final String prefix) throws ExecutionException, InterruptedException {

        final List<String> errorsKeys = this.s3Provider.listErrorsKeys(bucketErr, prefix);
        final List<Future<?>> executables = new ArrayList<>(errorsKeys.size());

        for (String errorKey : errorsKeys) {
            executables.add(executor.submit(() -> {
                try {
                    executeErrorsRecoveryForKey(bucketErr, errorKey);
                } catch (IOException e) {
                    throw new IllegalArgumentException(e);
                }
            }));
        }

        for (Future<?> future : executables)
            future.get();
    }

    private void executeErrorsRecoveryForKey(String bucketErr, String errorKey) throws IOException {

        final Error error = this.s3Provider.getError(bucketErr, errorKey);

        for (Entry entry : error.getEntries()) {

            final Pattern pattern = Pattern.compile(regex, Pattern.MULTILINE);
            final Matcher matcher = pattern.matcher(entry.getUrl());

            if (matcher.find()) {

                final String bucket = matcher.group(1);
                final String key = matcher.group(2);

                System.out.println(String.format("Processing %s/%s", bucket, key));
                processError(bucket, key);

            }
        }

        this.s3Provider.removeKey(bucketErr, errorKey);
    }


    private void processError(String bucket, String key) throws IOException {
        final List<String> data = this.s3Provider.getData(bucket, key);
        final List<List<String>> lists = ListUtils.partition(data, 500);

        for (List<String> list : lists) {

            List<Record> records = list.stream()
                    .map(e -> new Record().withData(ByteBuffer.wrap(e.getBytes())))
                    .collect(Collectors.toList());

            final PutRecordBatchRequest putRecordBatchRequest = new PutRecordBatchRequest();
            putRecordBatchRequest.setDeliveryStreamName(deliveryStreamName);
            putRecordBatchRequest.setRecords(records);

            this.amazonKinesisFirehose.putRecordBatch(putRecordBatchRequest);
        }

        this.s3Provider.removeKey(bucket, key);
    }
}
