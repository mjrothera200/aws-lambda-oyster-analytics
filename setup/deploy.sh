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
--zip-file fileb://function.zip --handler index.handler --runtime nodejs12.x \
--timeout 30 --memory-size 1024 \
--role arn:aws:iam::$AWS_ACCOUNTID:role/$ROOT-role

echo "Waiting for it to be created...."
sleep 60
echo "Finishing Process!"


# to do - maybe auto create the API

