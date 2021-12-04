# which aws
# aws --version

# python3 -m venv .venv
# source .venv/bin/activate
# pip install boto3 or pip3 install boto3
# pip install boto3[crt]

import boto3
import time

# S3 Document Data:
# YOU NEED TO CREATE S3 BUCKET AND ADD IMAGES TO IT THROUGH AWS CONSOLE
s3BucketVaccineCards = "demovaccinecards" 
vaccineCardFile = "card1.png"

# YOU NEED TO CREATE S3 BUCKET ONLY 
s3BucketTextractTextOutput = "demotextracttextoutputs" 
textractOutputFileName = "textract1.txt" # THIS IS FOR UPLOADING ONLY DON'T CREATE THIS FILE

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

# https://stackoverflow.com/questions/40336918/how-to-write-a-file-or-data-to-an-s3-object-using-boto3
def upload_txt_to_bucket(s3BucketName, content, filename):
    s3 = boto3.resource('s3')
    s3.Object(s3BucketName, filename).put(Body=content)

# Function invokes

textractText = ""

jobId = InvokeTextDetectJob(s3BucketVaccineCards, vaccineCardFile)
print("Started job with id: {}".format(jobId))
if(CheckJobComplete(jobId)):
    response = JobResults(jobId)
    for resultPage in response:
        for item in resultPage["Blocks"]:
            if item["BlockType"] == "LINE":
                textractText += item["Text"]
                textractText += "\n"
                #print ('\033[94m' + item["Text"] + '\033[0m ')
                #print (' \033[94m' + str(item["Confidence"]) + '\033[0m')
#print(textractText)
upload_txt_to_bucket(s3BucketTextractTextOutput, textractText, textractOutputFileName)


# Let's use Amazon S3
#s3 = boto3.resource('s3')

# Print out bucket names
#for bucket in s3.buckets.all():
    #print(bucket.name)

# Upload a new file
#data = open('test.jpg', 'rb')
#s3.Bucket('my-bucket').put_object(Key='test.jpg', Body=data)