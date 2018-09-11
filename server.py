#!/usr/bin/env python

import requests, zipfile, io, img2pdf, sys, os, time, logging, uuid, shutil

import hug
from hug_middleware_cors import CORSMiddleware

import subprocess
from tempfile import NamedTemporaryFile

from datetime import datetime, timedelta

from azure.storage.blob import (
    BlockBlobService,
    ContainerPermissions,
    BlobPermissions,
    PublicAccess,
    ContentSettings)
from applicationinsights import TelemetryClient

api = hug.API(__name__)
api.http.add_middleware(CORSMiddleware(api))

# create logger
logger = logging.getLogger('debuglogging')
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

@hug.get('/', output=hug.output_format.file)
def index():
    return "index.htm"

@hug.get('/static/{fn}', output=hug.output_format.file)
def static(fn):
    return 'static/{}'.format(fn)

@hug.post('/ocr')
def ocr(body, response, language: "The language(s) to use for OCR"="eng"):

    logger.info(os.environ)

    azureAccountName = os.environ['SCANOCR_STORAGE_ACCOUNT_NAME'] 
    azureAccountKey = os.environ['SCANOCR_STORAGE_ACCOUNT_KEY']
    appInsightsTelemetryKey = os.environ['SCANOCR_APP_INSIGHTS_TELEMETRY_KEY'] 
    
    fileSetId = body["file_set_id"]
    zipFileUrl = body["zip_file_url"]
    fileName = body["file_name"]

    # initiate app insights
    tc = TelemetryClient(appInsightsTelemetryKey)
    tc.context.operation.id = str(uuid.uuid4())
    tc.track_event('ocr', { 'zip_file_url': zipFileUrl, 'file_set_id': fileSetId })
    tc.flush()

    # download zip, extract
    zipRequestBody = requests.get(zipFileUrl)
    z = zipfile.ZipFile(io.BytesIO(zipRequestBody.content))
    tempDir = '/tmp/' + fileSetId + '/' + fileName
    if(os.path.isdir(tempDir)):
        shutil.rmtree(tempDir)
    z.extractall(tempDir)

    # grab all PNG images from zip extract results
    image_files = []
    for root, dirs, files in os.walk(tempDir):
        for file in files:
            if file.endswith(".png"):
                image_files.append(os.path.join(root, file))

    # log file count to app insights
    tc.track_event('ocr_zip_extracted', { 'file_count': len(image_files) })
    tc.flush()

    with open(tempDir + '/output.pdf', 'w+') as output:

        # convert PNGs to (non-OCR) PDF
        pdf_bytes = img2pdf.convert(image_files)
        file = open(tempDir + '/input.pdf',"wb")
        file.write(pdf_bytes)
        
        # log progress to app insights
        tc.track_event('ocr_pdf_created')
        tc.flush()
        
        # launch OCR process
        proc = subprocess.Popen( "ocrmypdf --jobs 4 --output-type pdf " + tempDir + "/input.pdf " + tempDir + "/output.pdf", stdin=subprocess.PIPE, shell=True )

    # wait for OCR completion
    code = proc.wait()

    # log OCR completion to app insights
    tc.track_event('ocr_output_pdf_complete')
    tc.flush()

    # upload resulting PDF to Azure
    blob_service = BlockBlobService(account_name=azureAccountName, account_key=azureAccountKey)
    blob_service.create_blob_from_path(
        'images/' + fileSetId,
        fileName + ".pdf",
        tempDir + '/output.pdf',
        content_settings=ContentSettings(content_type='application/pdf')
    )

    # log upload completion to app insights
    tc.track_event('ocr_uploaded_to_azure')
    tc.flush()

    # obtain download signature from Azure
    sas_url = blob_service.generate_blob_shared_access_signature('images/' + fileSetId,
        fileName + "pdf",
        BlobPermissions.READ,datetime.utcnow() + timedelta(hours=12))

    download_url = 'https://'+ azureAccountName +'.blob.core.windows.net/'+ 'images/' + fileSetId +'/'+ fileName + ".pdf" +'?'+sas_url

    # return results
    return {'filename': zipFileUrl, 'files': z.namelist(), 'download_url': download_url }
