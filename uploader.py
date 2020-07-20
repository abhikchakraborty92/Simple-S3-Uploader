import pandas as pd
import boto3
import os
import datetime
import time
from multiprocessing import Process
from threading import Thread

processes = []
file_threads = []

start = time.perf_counter()

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

def upload_s3_file(filename,fileaddress,s3bucket,s3_subfolder):
    error = 'No Error Found'
    start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        s3.upload_file(fileaddress,s3bucket,s3_subfolder)
        result = 'SUCCESS'
        end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print('\t[FILE UPLOAD ALERT:] %s upload to the SUBFOLDER %s inside the bucket %s SUCCESSFUL\n'%(str(filename),str(s3_subfolder),str(s3bucket)))
    except EnvironmentError as e:
        result = 'FAILURE'
        error = e
        #print(error)
        end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print('\t[FILE UPLOAD ALERT:] %s upload to the SUBFOLDER %s inside the bucket %s FAILED\n'%(str(filename),str(s3_subfolder),str(s3bucket)))
    
    log_tuple = (filename,fileaddress,s3bucket, s3_subfolder,start_time,end_time,result,error)
    log_df_uploader(log_tuple)

    return result


def upload_to_s3(filename,folderaddress,s3bucket,s3_subfolder):
    result = None 
    if os.path.isdir(folderaddress)==True:
        print(f'\n\n****{filename}****\n')
        time.sleep(1)
        print(f'Looking into contents...\n')
        contents = os.listdir(folderaddress)
        for item in contents:
            itemaddress = os.path.join(folderaddress,item)
            if os.path.isdir(itemaddress):
                #result = upload_to_s3(item,itemaddress,s3bucket,s3_subfolder+'/'+item)
                th = Thread(target=upload_to_s3, args=[item,itemaddress,s3bucket,s3_subfolder+'/'+item])
                th.start()
                file_threads.append(th)
            else:
                #result = upload_s3_file(item,itemaddress,s3bucket,s3_subfolder+'/'+item)
                th = Thread(target=upload_to_s3, args=[item,itemaddress,s3bucket,s3_subfolder+'/'+item])
                th.start()
                file_threads.append(th)
    else:
        result = upload_s3_file(filename,folderaddress,s3bucket,s3_subfolder)
    
    # for file_thd in file_threads:
    #     file_thd.join()
    
    return result


if __name__ == '__main__':
    # Looping through filelist
    for row,index in filelist.iterrows():
        fileaddress = os.path.join(filelist['filepath'][row],filelist['filename'][row])

        if filelist['s3_subfolder_path'][row] is None:
            filelist['s3_subfolder_path'][row] = filelist['filename'][row]
        else:
            filelist['s3_subfolder_path'][row] = str(os.path.join(filelist['s3_subfolder_path'][row],filelist['filename'][row])).replace('\\','/')

        thd = Process(target=upload_to_s3, args=[filelist['filename'][row],fileaddress,filelist['s3bucket'][row],filelist['s3_subfolder_path'][row]])
        #upload_to_s3(filelist['filename'][row],fileaddress,filelist['s3bucket'][row],filelist['s3_subfolder_path'][row])
        thd.start()
        processes.append(thd)

    for prcs in processes:
        prcs.join()

    end = time.perf_counter()

    print(f'Process Completed in {round(end-start,2)} seconds')


