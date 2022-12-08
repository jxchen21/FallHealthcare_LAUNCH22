import pydicom
import argparse
import sqlite3
import json

#Ingestion script
parser = argparse.ArgumentParser(description='DICOM File Reader')
parser.add_argument('-f',dest='filepath',action='store',help='Input Filepath')
args = parser.parse_args()
dcm = pydicom.dcmread(args.filepath)

dicomDict = {'AccessionNumber':None,'StudyDescription':None,'SeriesDescription':None,
'StudyInstanceUID':None,'SeriesInstanceUID':None,'Manufacturer':None,'Modality':None}

#Function to create dictionary entries
def dataEntry(dcm, field):
    if(hasattr(dcm,field)):
        name = dcm.get(field)
    else:
        name = ''
    return name

connection = sqlite3.connect('dicom_data.db')
cursor = connection.cursor()

cursor.execute('''CREATE TABLE IF NOT EXISTS dicom(dicom_id INTEGER PRIMARY KEY, AccessionNumber TEXT, 
StudyDescription TEXT, SeriesDescription TEXT, StudyInstanceUID INTEGER, SeriesInstanceUID INTEGER, 
Manufacturer TEXT, Modality TEXT)''')

cursor.execute('CREATE TABLE IF NOT EXISTS filepath(dicom_id INTEGER PRIMARY KEY, filepath TEXT)')
cursor.execute('INSERT INTO filepath(filepath) VALUES (?)',(args.filepath,))

sqlItems = []

#Creates dictionary entries
for key in dicomDict:
    data = dataEntry(dcm,key)
    dicomDict[key] = data 
    sqlItems.append(data)
    
#inserts dicom data into table
cursor.execute('''INSERT INTO dicom (AccessionNumber,StudyDescription,SeriesDescription,StudyInstanceUID,
SeriesInstanceUID,Manufacturer,Modality) VALUES (?,?,?,?,?,?,?)''',(sqlItems[0],sqlItems[1],sqlItems[2],
sqlItems[3],sqlItems[4],sqlItems[5],sqlItems[6]))

#Creates json database
with open('dicomDict.json', 'w') as outfile:
    json.dump(dicomDict, outfile)

connection.commit()
connection.close()
