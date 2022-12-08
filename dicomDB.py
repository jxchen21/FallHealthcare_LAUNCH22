import pymysql 
import argparse
import boto3
import os
import pydicom
AWSHOST = 'Insert host URL'
PORT = 'Insert AWS port number'
USER = 'Insert Username'
PASSWD = 'Insert Password'

dicomDict = {'AccessionNumber':None,'StudyDescription':None,'SeriesDescription':None,
'StudyInstanceUID':None,'SeriesInstanceUID':None,'Manufacturer':None,'Modality':None}

def dataEntry(dcm, field):
    if(hasattr(dcm,field)):
        name = dcm.get(field)
    else:
        name = ''
    return name

#Parser
parser = argparse.ArgumentParser(description='DICOM File Reader')
parser.add_argument('-f',dest='filepath',action='store',help='Input Filepath')
parser.add_argument('-b',dest='bucketpath',action='store',help='Desired Bucket Directory')
args = parser.parse_args()

#Connect to database
initdb = pymysql.connect(host=AWSHOST,port=PORT,user=USER,passwd=PASSWD)
dbcur = initdb.cursor()
dbcur.execute('CREATE DATABASE IF NOT EXISTS dicomdb')
dbcur.close()
db = pymysql.connect(host=AWSHOST,port=PORT,user=USER,passwd=PASSWD,db='dicomdb')
cur = db.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS dicom(dicom_id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY, AccessionNumber TEXT, 
        StudyDescription TEXT, SeriesDescription TEXT, StudyInstanceUID INTEGER, SeriesInstanceUID INTEGER, 
        Manufacturer TEXT, Modality TEXT)''')
cur.execute('CREATE TABLE IF NOT EXISTS filepath(dicom_id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY, filepath TEXT)')

#Create bucket
session = boto3.Session(aws_access_key_id="Insert Access Key",
    aws_secret_access_key="Insert Secret Access Key",)
s3 = session.client('s3')
BUCKET = "Your Bucket Name"
if BUCKET not in s3.list_buckets():
    s3.create_bucket(Bucket=BUCKET)

#Get filename for boto
if('\\' in args.filepath):
    filelist = args.filepath.split('\\')
elif('/' in args.filepath):
    filelist = args.filepath.split('/')
fileDir = filelist[-3]+'/'+filelist[-2]
bucketchecker = session.resource('s3')
bucket = bucketchecker.Bucket(BUCKET)
curkeys = []
for file in bucket.objects.all():
    curkeys.append(file.key)

for file in os.listdir(args.filepath):
    sqlItems = []
    full = os.path.join(args.filepath, file) 

    
    if os.path.isfile(full):
        dcm = pydicom.dcmread(full)
        if(args.bucketpath != None):
            botodir = args.bucketpath+"/"+str(file)
        else:
            botodir = fileDir+"/"+str(file)
        #Upload to bucket
        if(fileDir+"/"+str(file) not in curkeys):
            s3.upload_file(full, BUCKET,botodir)
        #Add filepath to SQL table
        cur.execute('INSERT INTO filepath(filepath) VALUES (%s)',(botodir))

        for key in dicomDict:
            data = dataEntry(dcm,key)
            dicomDict[key] = data 
            sqlItems.append(data)

        cur.execute('''INSERT INTO dicom (AccessionNumber,StudyDescription,SeriesDescription,StudyInstanceUID,
            SeriesInstanceUID,Manufacturer,Modality) VALUES (%s,%s,%s,%s,%s,%s,%s)''',(sqlItems[0],sqlItems[1],sqlItems[2],
            sqlItems[3],sqlItems[4],sqlItems[5],sqlItems[6]))
db.commit()
cur.close()