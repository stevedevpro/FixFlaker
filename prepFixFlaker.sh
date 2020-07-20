#!/usr/bin/bash

yum update -y
yum install -y python3
yum install –y https://s3.amazonaws.com/streaming-data-agent/aws-kinesis-agent-latest.amzn1.noarch.rpm
if test -f "/etc/aws-kinesis/agent.json"; then
	mv /etc/aws-kinesis/agent.json /etc/aws-kinesis/agent.json.`date +"%Y-%m-%dT%H:%M:%S.%3N"`
fi
echo '{
  "assumeRoleExternalId": "arn:aws:iam::253375139526:role/delegate_kinesis",
  "cloudwatch.emitMetrics": false,
  "kinesis.endpoint": "kinesis.us-east-1.amazonaws.com",
  "firehose.endpoint": "firehose.us-east-1.amazonaws.com",
  
  "flows": [
    {
      "filePattern": "/tmp/fix.log.*",
      "kinesisStream": "datastream01",
      "partitionKeyOption": "RANDOM",
      "maxBufferAgeMillis": "1000",
      "dataProcessingOptions": [
                {
                    "optionName": "CSVTOJSON",
                    "customFieldNames": [ "timestamp", "rawFix" ],
                    "delimiter": " "
                }
            ]
    },
    {
      "filePattern": "/tmp/fix.log.__*",
      "deliveryStream": "firehose01xx"
    }
  ]
}' > /etc/aws-kinesis/agent.json

service aws-kinesis-agent start
chkconfig aws-kinesis-agent on

