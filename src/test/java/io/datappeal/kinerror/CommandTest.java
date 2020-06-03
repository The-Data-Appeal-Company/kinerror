package io.datappeal.kinerror;

import io.datappeal.kinerror.cmd.Command;
import org.apache.commons.cli.ParseException;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CommandTest {

    @Test
    public void shouldGenerateCommand() throws ParseException {

        final Command command = Command.CommandBuilder.fromArgs(new String[]{"-b=bucket", "-d=stream", "-r=region",
                "--access_key_id=ACCESS_KEY_ID", "--secret_key=SECRET_KEY"});

        assertThat(command.getAwsCredentialsProvider().getCredentials().getAWSAccessKeyId()).isEqualTo("ACCESS_KEY_ID");
        assertThat(command.getAwsCredentialsProvider().getCredentials().getAWSSecretKey()).isEqualTo("SECRET_KEY");

        assertThat(command.getConcurrency()).isEqualTo(1);
        assertThat(command.getKinesisFirehoseDeliveryStreamName()).isEqualTo("stream");
        assertThat(command.getS3Bucket()).isEqualTo("bucket");
        assertThat(command.getS3BucketPrefix()).isNull();
        assertThat(command.getKinesisFirehoseRegion()).isEqualTo("region");

    }
}
