package io.datappeal.kinerror.cmd;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import org.apache.commons.cli.*;

public class Command {

    private final AWSCredentialsProvider awsCredentialsProvider;
    private final String s3Bucket;
    private final String s3BucketPrefix;
    private final String kinesisFirehoseDeliveryStreamName;
    private final String kinesisFirehoseRegion;
    private final int concurrency;


    private Command(final AWSCredentialsProvider awsCredentialsProvider, final String s3Bucket,
                    final String s3BucketPrefix, final String kinesisFirehoseDeliveryStreamName,
                    final String kinesisFirehoseRegion, final int concurrency) {
        this.awsCredentialsProvider = awsCredentialsProvider;
        this.s3Bucket = s3Bucket;
        this.s3BucketPrefix = s3BucketPrefix;
        this.kinesisFirehoseDeliveryStreamName = kinesisFirehoseDeliveryStreamName;
        this.kinesisFirehoseRegion = kinesisFirehoseRegion;
        this.concurrency = concurrency;
    }

    public AWSCredentialsProvider getAwsCredentialsProvider() {
        return awsCredentialsProvider;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public String getS3BucketPrefix() {
        return s3BucketPrefix;
    }

    public String getKinesisFirehoseDeliveryStreamName() {
        return kinesisFirehoseDeliveryStreamName;
    }

    public String getKinesisFirehoseRegion() {
        return kinesisFirehoseRegion;
    }

    public int getConcurrency() {
        return concurrency;
    }

    public static class CommandBuilder {

        public static Command fromArgs(String[] args) throws ParseException {

            final Options options = new Options();
            options.addOption(new Option("ak", "access_key_id", true, "AWS access key id"));
            options.addOption(new Option("sk", "secret_key", true, "AWS secret key"));
            options.addOption(new Option("b", "bucket-name", true, "S3 bucket name"));
            options.addOption(new Option("p", "prefix", true, "S3 prefix"));
            options.addOption(new Option("c", "concurrency", true, "The concurrency of process"));
            options.addOption(new Option("r", "region", true, "AWS region"));
            options.addOption(new Option("d", "delivery-stream-name", true, "The output delivery stream name"));


            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);


            final String bucket;
            final String deliveryStreamName;


            if (cmd.hasOption("b") && cmd.hasOption("d")) {
                bucket = cmd.getOptionValue("b");
                deliveryStreamName = cmd.getOptionValue("d");
            } else {
                throw new RuntimeException("params required -b (--bucket-name) and -d(--delivery-stream-name)");
            }

            String prefix = null;

            if (cmd.hasOption("p")) {
                prefix = cmd.getOptionValue("p");
            }

            AWSCredentialsProvider credentialsProvider;

            if (cmd.hasOption("ak") && cmd.hasOption("sk")) {

                String ak = cmd.getOptionValue("ak");
                String sk = cmd.getOptionValue("sk");

                final BasicAWSCredentials awsCreds = new BasicAWSCredentials(ak, sk);
                credentialsProvider = new AWSStaticCredentialsProvider(awsCreds);

            } else {
                credentialsProvider = new ProfileCredentialsProvider();
            }

            int concurrency = 1;

            if (cmd.hasOption("c")) {
                concurrency = Integer.parseInt(cmd.getOptionValue("c"));
            }

            final String region = cmd.getOptionValue("r");

            return new Command(credentialsProvider, bucket, prefix, deliveryStreamName, region, concurrency);
        }
    }

}
