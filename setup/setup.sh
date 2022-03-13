#!/bin/sh

# Set these variables for your installation
ROOT=oyster-haven-analytics
AWS_ACCOUNTID=922129242138


# Derived Variables for Script

POLICY_NAME=$ROOT-policy
# Note check the file to ensure the bucket names are correct
POLICY_FILE_NAME=$ROOT-policy.json
POLICY_ARN=arn:aws:iam::$AWS_ACCOUNTID:policy/$POLICY_NAME
EXECUTION_ROLE_NAME=$ROOT-role
# Check the account id in this file
EXECUTION_ROLE_NAME_TRUST_POLICY=$EXECUTION_ROLE_NAME-trust-policy.json


echo "Starting Setup"

aws iam create-policy --policy-name $POLICY_NAME --policy-document file://$POLICY_FILE_NAME

# to do - create a role and attach the polcies

aws iam create-role --role-name $EXECUTION_ROLE_NAME --assume-role-policy-document '{"Version": "2012-10-17","Statement": [{ "Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}'

aws iam attach-role-policy --policy-arn $POLICY_ARN --role-name $EXECUTION_ROLE_NAME
aws iam attach-role-policy --policy-arn arn:aws:iam::aws:policy/AmazonTimestreamFullAccess --role-name $EXECUTION_ROLE_NAME

# verify it completed
aws iam get-role --role-name $EXECUTION_ROLE_NAME
aws iam list-role-policies --role-name $EXECUTION_ROLE_NAME


