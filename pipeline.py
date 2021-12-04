# which aws
# aws --version

# python3 -m venv .venv
# source .venv/bin/activate
# pip install boto3 or pip3 install boto3
# pip install boto3[crt]

import boto3
import time

## Textract APIs used - "start_document_text_detection", "get_document_text_detection"
def InvokeTextDetectJob(s3BucketName, objectName):
    response = None
    client = boto3.client('textract')
    response = client.start_document_text_detection(
            DocumentLocation={
                      'S3Object': {
                                    'Bucket': s3BucketName,
                                    'Name': objectName
                                }
           })
    return response["JobId"]

def CheckJobComplete(jobId):
    time.sleep(5)
    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
    status = response["JobStatus"]
    print("Job status: {}".format(status))
    while(status == "IN_PROGRESS"):
        time.sleep(5)
        response = client.get_document_text_detection(JobId=jobId)
        status = response["JobStatus"]
        print("Job status: {}".format(status))
    return status

def JobResults(jobId):
    pages = []
    client = boto3.client('textract')
    response = client.get_document_text_detection(JobId=jobId)
 
    pages.append(response)
    print("Resultset page recieved: {}".format(len(pages)))
    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']
        while(nextToken):
            response = client.get_document_text_detection(JobId=jobId, NextToken=nextToken)
            pages.append(response)
            print("Resultset page recieved: {}".format(len(pages)))
            nextToken = None
            if('NextToken' in response):
                nextToken = response['NextToken']
    return pages

# S3 Document Data
s3BucketName = "demovaccinecards"
documentName = "card1.png"

# Function invokes
jobId = InvokeTextDetectJob(s3BucketName, documentName)
print("Started job with id: {}".format(jobId))
if(CheckJobComplete(jobId)):
    response = JobResults(jobId)
    for resultPage in response:
        for item in resultPage["Blocks"]:
            if item["BlockType"] == "LINE":
                print ('\033[94m' + item["Text"] + '\033[0m ')
                print (' \033[94m' + str(item["Confidence"]) + '\033[0m')

# Let's use Amazon S3
#s3 = boto3.resource('s3')

# Print out bucket names
#for bucket in s3.buckets.all():
    #print(bucket.name)

# Upload a new file
#data = open('test.jpg', 'rb')
#s3.Bucket('my-bucket').put_object(Key='test.jpg', Body=data)