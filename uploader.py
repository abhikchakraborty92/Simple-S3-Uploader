import pandas as pd
import boto3
import os
import datetime
import time
from multiprocessing import Process
from threading import Thread

processes = []      # Processes are stored in this list

start = time.perf_counter() # Starting counter to track time

print('***S3 FILE UPLOADER***\n')

# Reading the file for getting the details to files to upload on S3
filelist = None
try:
    filelist = pd.read_csv('filelist.csv')
except:
    print('No file to read')

# File to write logs regarding the upload
try:
    uploadlog = pd.read_csv('uploadlog.csv')
    upload_columns = uploadlog.columns         # Getting column list to upload logs later with relevant column mapping
except:
    print('No file to read')

# This function is used to log the details regarding the s3 uploads
def log_df_uploader(log_tuple):
    templist = []
    templist.append(log_tuple)
    log_df = pd.DataFrame(templist,columns=upload_columns)
    try:
        log_df.to_csv('uploadlog.csv',header=False,index=False,mode='a')
        status = 'Logging successful'
    except:
        status = 'Logging failed'
    return status

# Creating a boto3 client
# For this AWS CLI has to be installed and a relevant profile with required permissions have to be created
s3 = boto3.client('s3')

# This function is for uploading S3 files
def upload_s3_file(filename,fileaddress,s3bucket,s3_subfolder):
    error = 'No Error Found'
    start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        s3.upload_file(fileaddress,s3bucket,s3_subfolder)
        result = 'SUCCESS'
        end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print('\t[FILE UPLOAD SUCCESSFUL:] FILE: %s | SUBFOLDER: %s | BUCKET: %s\n'%(str(filename),str(s3_subfolder),str(s3bucket)))
    except EnvironmentError as e:
        result = 'FAILURE'
        error = e
        #print(error)
        end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print('\t[FILE UPLOAD FAILED:] FILE: %s | SUBFOLDER: %s | BUCKET: %s\n'%(str(filename),str(s3_subfolder),str(s3bucket)))
    
    log_tuple = (filename,fileaddress,s3bucket, s3_subfolder,start_time,end_time,result,error)
    log_df_uploader(log_tuple)

    return result

# This function handles S3 files and folders. Whenever we have a folder to upload, 
# it gathers the contents of the folder and creates multiple threads to upload them almost parallel
def upload_to_s3(filename,folderaddress,s3bucket,s3_subfolder):
    result = None 
    if os.path.isdir(folderaddress)==True:
        print(f'\n\n****{filename}****\n')
        time.sleep(1)
        print(f'Looking into contents...\n')
        contents = os.listdir(folderaddress)
        for item in contents:
            itemaddress = os.path.join(folderaddress,item)
            if os.path.isdir(itemaddress): # if it is a directory
                
                # Creating multiple threads for file contents
                th = Thread(target=upload_to_s3, args=[item,itemaddress,s3bucket,s3_subfolder+'/'+item])
                th.start()
                
            else: # if it is a file
                
                # Creating multiple threads for file contents
                th = Thread(target=upload_to_s3, args=[item,itemaddress,s3bucket,s3_subfolder+'/'+item])
                th.start()
                
    else:
        result = upload_s3_file(filename,folderaddress,s3bucket,s3_subfolder)
    
    
    return result


if __name__ == '__main__':
    
    # Looping through filelist csv
    for row,index in filelist.iterrows():
        fileaddress = os.path.join(filelist['filepath'][row],filelist['filename'][row])

        if filelist['s3_subfolder_path'][row] is None:
            filelist['s3_subfolder_path'][row] = filelist['filename'][row]
        else:
            filelist['s3_subfolder_path'][row] = str(os.path.join(filelist['s3_subfolder_path'][row],filelist['filename'][row])).replace('\\','/')


# For every row in the filelist file, we are creating a process and executing them at the same time
        thd = Process(target=upload_to_s3, args=[filelist['filename'][row],fileaddress,filelist['s3bucket'][row],filelist['s3_subfolder_path'][row]])
        
        thd.start()
        processes.append(thd)

    for prcs in processes:
        prcs.join()

    end = time.perf_counter()

    print(f'Process Completed in {round(end-start,2)} seconds')


