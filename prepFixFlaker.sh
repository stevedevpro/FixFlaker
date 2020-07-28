#!/bin/bash
yum update --assumeyes
yum install --assumeyes python3
yum install --assumeyes https://s3.amazonaws.com/streaming-data-agent/aws-kinesis-agent-latest.amzn1.noarch.rpm
if test -f "/etc/aws-kinesis/agent.json"; then
	mv /etc/aws-kinesis/agent.json /etc/aws-kinesis/agent.json.`date +"%Y-%m-%dT%H:%M:%S.%3N"`
fi
echo '{
  "assumeRoleExternalId": "arn:aws:iam::764544309252:role/FirehoseDelegatedProducerRole",
  "cloudwatch.emitMetrics": false
  "flows": [
    {
      "filePattern": "/tmp/fix.log.*",
      "deliveryStream": "mfm_kfh_collect_fixlog",
      "maxBufferAgeMillis": "1000",
      "dataProcessingOptions": [
                {
                    "optionName": "CSVTOJSON",
                    "customFieldNames": [ "fixlog_timestamp", "fixlog_session", "fixlog_message" ],
                    "delimiter": " "
                }
            ]
    }
  ]
}' > /etc/aws-kinesis/agent.json

service aws-kinesis-agent start
chkconfig aws-kinesis-agent on

if test -f "/root/FixFlaker.py"; then
	mv /root/FixFlaker.py /root/FixFlaker.py.`date +"%Y-%m-%dT%H:%M:%S.%3N"`
fi
curl https://raw.githubusercontent.com/stevedevpro/FixFlaker/master/FixFlaker.py > /root/FixFlaker.py
python3 /root/FixFlaker.py
echo "done"