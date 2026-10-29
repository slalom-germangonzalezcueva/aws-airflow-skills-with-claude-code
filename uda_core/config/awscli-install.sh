#!/bin/bash
zip_file="awscliv2.zip"
cd /tmp
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "$zip_file"
unzip "$zip_file"
./aws/install
rm $zip_file
aws --version
