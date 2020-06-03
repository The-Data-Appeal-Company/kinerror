package io.datappeal.kinerror;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.datappeal.kinerror.importer.Importer;
import io.datappeal.kinerror.s3.S3Provider;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

public class ImporterTest {

    private AmazonS3 amazonS3;
    private AmazonKinesisFirehose amazonKinesisFirehose;

    @Before
    public void setUp() {

        this.amazonS3 = mock(AmazonS3.class);
        this.amazonKinesisFirehose = mock(AmazonKinesisFirehose.class);

    }


    @Test
    public void shouldExecuteJob() throws ExecutionException, InterruptedException {

        final String bucketName = "bucket";
        final String prefix = "prefix";

        final String errorFileKey = "urls-prod-err.json";
        final String fileKey = "urls-prod-58-2020-03-15-05-00-58-23ac1aa7-c186-4f35-8966-9513c2dbc449.json";


        ListObjectsV2Result objectListing = new ListObjectsV2Result();
        objectListing.setBucketName(bucketName);
        objectListing.setPrefix(prefix);


        final S3ObjectSummary s3ObjectSummary = new S3ObjectSummary();
        s3ObjectSummary.setKey(errorFileKey);
        objectListing.getObjectSummaries().add(s3ObjectSummary);

        when(this.amazonS3.listObjectsV2(bucketName, prefix)).thenAnswer(i -> objectListing);

        InputStream targetStreamErr = TestResources.getResource("data/" + errorFileKey);

        final S3Object s3ObjectErr = new S3Object();
        s3ObjectErr.setBucketName(bucketName);
        s3ObjectErr.setKey(errorFileKey);
        s3ObjectErr.setObjectContent(targetStreamErr);

        when(this.amazonS3.getObject(bucketName, errorFileKey)).thenAnswer(i -> s3ObjectErr);


        final InputStream targetStream = TestResources.getResource("data/" + fileKey);

        final S3Object s3Object = new S3Object();
        s3Object.setBucketName("travelappeal-kinesis-firehose-eu");
        s3Object.setKey("urls-prod2020/04/01/22/urls-prod-58-2020-03-15-05-00-58-23ac1aa7-c186-4f35-8966-9513c2dbc449.json");
        s3Object.setObjectContent(targetStream);


        when(this.amazonS3.getObject("travelappeal-kinesis-firehose-eu", "urls-prod2020/04/01/22/urls-prod-58-2020-03-15-05-00-58-23ac1aa7-c186-4f35-8966-9513c2dbc449.json")).thenAnswer(i -> s3Object);


        final ArgumentCaptor<PutRecordBatchRequest> captor = ArgumentCaptor.forClass(PutRecordBatchRequest.class);

        new Importer(new S3Provider(amazonS3), amazonKinesisFirehose, "streammo", 2)
                .process(bucketName, prefix);

        verify(this.amazonKinesisFirehose, times(3)).putRecordBatch(captor.capture());
        List<PutRecordBatchRequest> allValues = captor.getAllValues();


        assertThat(allValues.get(0).getDeliveryStreamName()).isEqualTo("streammo");
        assertThat(allValues.get(0).getRecords()).hasSize(500);

        assertThat(allValues.get(1).getDeliveryStreamName()).isEqualTo("streammo");
        assertThat(allValues.get(1).getRecords()).hasSize(500);

        assertThat(allValues.get(2).getDeliveryStreamName()).isEqualTo("streammo");
        assertThat(allValues.get(2).getRecords()).hasSize(299);


    }
}
