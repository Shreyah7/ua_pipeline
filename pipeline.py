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

## Comprehend APIs used
def InvokeComprehendJob(inputText):
    response = None
    client = boto3.client('comprehend')
    response = client.detect_entities(
            Text=inputText,
            LanguageCode='en',
    )
    return response

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

def runTextract():
    text = ""
    textractJobId = InvokeTextDetectJob(s3BucketVaccineCards, vaccineCardFile)
    print("Started job with id: {}".format(textractJobId))
    if(CheckJobComplete(textractJobId)):
        response = JobResults(textractJobId)
        for resultPage in response:
            for item in resultPage["Blocks"]:
                if item["BlockType"] == "LINE":
                    text += item["Text"]
                    text += "\n"
                    #print ('\033[94m' + item["Text"] + '\033[0m ')
                    #print (' \033[94m' + str(item["Confidence"]) + '\033[0m')
    #print(textractText)
    return text
    #upload_txt_to_bucket(s3BucketTextractTextOutput, textractText, textractOutputFileName)

# IDENTIFY NAMES
def runComprehension(textractText):
    response = InvokeComprehendJob(textractText)
    for entity in response['Entities']:
        if (entity["Type"] == "PERSON"):
            print(entity["Text"])

#textractText = "COVID-19 Vaccination Record Card\n Please keep this record card, which includes medical \
#                information\n <\nCDC.\nabout the vaccines you have received.\n***\n-\nPor favor, guarde esta \
#                tarjeta de registro, que incluye información\nmédica sobre las vacunas que ha recibido.\nPrasad, \
#                \nLast Name\nShreyah\nFirst Name\nMI\n8-3-2001\nDate of birth\nPatient number (medical record \
#                or IIS record number)\nProduct Name/Manufacturer\nHealthcare Professional\nVaccine\nDate\nLot Number \
#                \nor Clinic Site\n1st Dose\nAfizer\7/19/21\nCOVID-19\nmm dd yy\nwaguzss\nFA6780\n2nd Dose\nPfizer \
#                \n8 11 a\nWalgrees\nCOVID-19\nPA7484\nmm dd yy\n9355"
textractText = runTextract()
runComprehension(textractText)


