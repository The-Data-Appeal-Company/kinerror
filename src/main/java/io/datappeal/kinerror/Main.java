package io.datappeal.kinerror;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import io.datappeal.kinerror.cmd.Command;
import io.datappeal.kinerror.importer.Importer;
import io.datappeal.kinerror.s3.S3Provider;
import org.apache.commons.cli.ParseException;

import java.util.concurrent.ExecutionException;


public class Main {


    public static void main(String[] args) throws ParseException, ExecutionException, InterruptedException {

        final Command command = Command.CommandBuilder.fromArgs(args);

        final AmazonKinesisFirehose firehose = AmazonKinesisFirehoseClientBuilder.standard()
                .withCredentials(command.getAwsCredentialsProvider())
                .withRegion(Regions.fromName(command.getKinesisFirehoseRegion()))
                .build();

        final AmazonS3 amazonS3 = AmazonS3ClientBuilder.standard()
                .withCredentials(command.getAwsCredentialsProvider())
                .build();

        final S3Provider s3Provider = new S3Provider(amazonS3);


        new Importer(s3Provider, firehose, command.getKinesisFirehoseDeliveryStreamName(), command.getConcurrency())
                .process(command.getS3Bucket(), command.getS3BucketPrefix());


    }
}
