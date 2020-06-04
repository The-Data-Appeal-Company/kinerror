# Kinerror
### Simple and concurrently cli to process AWS Kinesis firehose errors
Kinerror is a simple command line tool usable to process errors about kinesis firehose. If you have some errors on your stream and the delivery retry time is over, you need to reimport your data manually. 
We don't want you to do this!
### Usage

Just lunch the cli from terminal. Parameters supported is:

|Short opt  | Long opt  | Description   |  
|---|---|---|
| -ak|--access-key-id  |  The AWS access key id. If not specified the tool use the default authenticator specified in `~/.aws/credentials` |
| -sk|--secret-key  | The AWS secret key. If not specified the tool use the default authenticator specified in `~/.aws/credentials`  |
|-b |--bucket-name | The Kinesis firehose error s3 bucket    |
|-p |--bucket-prefix | The prefix usable to filter errors in bucket error    |
|-c |--concurrency | The max number of error files processed concurrently    |
|-r |--region | The AWS region of the Kinesis Firehose delivery stream    |
|-d |--delivery-stream-name | The AWS Kinesis Firehose delivery stream name    |

### Examples
#### Process all error of a firehose stream

```bash
kinerror -b <bucketErrName> -d <deliveryStreamName> -r <deliveryStreamRegion>
```

#### Process all error of a firehose stream with concurrency 10

```bash
kinerror -c 10 -b <bucketErrName> -d <deliveryStreamName> -r <deliveryStreamRegion>
```

#### Process error in prefix 2020/05 of a firehose stream

```bash
kinerror -b <bucketErrName> -p 2020/05 -d <deliveryStreamName> -r <deliveryStreamRegion>
```