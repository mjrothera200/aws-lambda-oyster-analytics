#!/bin/sh

# Set these variables for your installation
ROOT=oyster-haven-analytics
AWS_ACCOUNTID=922129242138

# Remove any previous function.zip
rm -rf function.zip

cd ../lambda-analytics
zip -r function.zip .
cd ../setup
mv ../lambda-analytics/function.zip .

aws lambda create-function --function-name $ROOT \
--zip-file fileb://function.zip --handler index.handler --runtime nodejs16.x \
--timeout 30 --memory-size 1024 \
--role arn:aws:iam::$AWS_ACCOUNTID:role/$ROOT-role

echo "Waiting for it to be created...."
sleep 60
echo "Finishing Process!"

# setup the trigger - Note change to 'rate(7 days)'
aws events put-rule \
--name $ROOT-rule \
--schedule-expression 'rate(7 days)'

aws lambda add-permission \
--function-name $ROOT \
--statement-id $ROOT-event \
--action 'lambda:InvokeFunction' \
--principal events.amazonaws.com \
--source-arn arn:aws:events:us-east-1:$AWS_ACCOUNTID:rule/$ROOT-rule

aws events put-targets --rule $ROOT-rule --targets file://$ROOT-targets.json
