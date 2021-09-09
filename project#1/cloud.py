import os
import sys
import boto3
import json
import random
import string
from basic_defs import cloud_storage, NAS
from azure.storage.blob import BlobServiceClient
from google.cloud import storage
from google.oauth2 import service_account

##############################################################################

class AWS_S3(cloud_storage):
    def __init__(self):
        self.access_key_id = "AKIAZ3WFZEEF2ARUKYJL"
        self.access_secret_key = "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"
        self.bucket_name = "csce678-s21-p1-323000675"
        self.S3_ = boto3.resource('s3', aws_access_key_id = self.access_key_id, aws_secret_access_key = self.access_secret_key)
        self.my_bucket = self.S3_.Bucket(self.bucket_name)
        self.tmp_dir = 'tmp'

    #TODO
    def list_blocks(self):
        list_ = []
        for my_bucket_object in self.my_bucket.objects.all():
            list_.append(str(my_bucket_object.key))
        ret = []
        for b in list_:
            if "_mohammad_" in b:
                ret.append(b)
            else:
                ret.append(int(b)) 
        return ret

    def read_block(self, offset):
        test = False
        if isinstance(offset, int):
            test = True
        offset = str(offset)
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        downloaded_file = self.tmp_dir + '/' + str(offset) 
        self.my_bucket.download_file(offset, downloaded_file )
        if os.path.exists(downloaded_file):
            with open(downloaded_file, "rb") as binaryfile :
                myArr = bytearray(binaryfile.read())
                os.remove(downloaded_file)
                return myArr

    def write_block(self, block, offset):
        offset = str(offset)
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        uploading_file = self.tmp_dir + '/' + str(offset)
        with open(uploading_file, "wb") as newFile:
            newFile.write(block)
        self.my_bucket.upload_file(Filename = uploading_file, Key = offset)
        os.remove(uploading_file)

    def delete_block(self, offset):
        test = False
        if isinstance(offset, int):
            test = True
        offset = str(offset)
        list_ = self.list_blocks()
        if test == True:
            if int(offset) in list_:
                my_object = self.S3_.Object(self.bucket_name, str(offset))
                my_object.delete()
        else:
            if offset in list_:
                my_object = self.S3_.Object(self.bucket_name, str(offset))
                my_object.delete()
##############################################################################

class Azure_Blob_Storage(cloud_storage):
    def __init__(self):
        self.key = ""
        self.conn_str = 'DefaultEndpointsProtocol=https;AccountName=csce678s21;AccountKey=u04DPr/UGGADYcl27vrXG3lAZ7cMP7LC+4Y3NKuR3nL8jLkp0xwG9NRzfCtDHG2nn4xX4adldrHfFmhRtT3afA==;EndpointSuffix=core.windows.net'
        self.account_name = "csce678s21"
        self.container_name = "csce678-s21-p1-323000675"
        self.blob_service_client = BlobServiceClient.from_connection_string(self.conn_str)
        self.container_client = self.blob_service_client.get_container_client(self.container_name)
        self.tmp_dir = 'tmp'

    #TODO
    def list_blocks(self):
        list_ = []
        blobs_list = self.container_client.list_blobs()
        for blob in blobs_list:
            list_.append(str(blob.name))
        ret = []
        for b in list_:
            if "_mohammad_" in b:
                #return string if called from NAS
                ret.append(b)
            else:
                #returin int if called from TEST
                ret.append(int(b)) 
        return ret

    def read_block(self, offset):
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        test = False
        if isinstance(offset, int):
            test = True
        offset = str(offset)
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        #first check if the block exist
        if test==True:
            temp_list = self.list_blocks()
            if int(offset) not in temp_list:
               dummy = []
               return dummy
        else:
            temp_list = self.list_blocks()
            if str(offset) not in temp_list:
                dummy = []
                return dummy
        #the block exists so we can download it
        #note for downloading we always pass offset as string
        downloaded_file = self.tmp_dir + '/' + str(offset) 
        blob_client = self.container_client.get_blob_client(str(offset))
        with open(downloaded_file, "wb") as my_blob:
            download_stream = blob_client.download_blob()
            my_blob.write(download_stream.readall())
        if os.path.exists(downloaded_file):
            with open(downloaded_file, "rb") as binaryfile :
                myArr = bytearray(binaryfile.read())
                os.remove(downloaded_file)
                return myArr

    def write_block(self, block, offset):
        test = False
        if isinstance(offset, int):
            test = True
        offset = str(offset)
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        #if block of same offset already exists, delete it first
        if test==True:
            temp_list = self.list_blocks()
            if int(offset) in temp_list:
                self.delete_block(int(offset))
        else:
            temp_list = self.list_blocks()
            if str(offset) in temp_list:
                self.delete_block(str(offset))
        #when deletion done upload now
        #note when uploading offset is always string
        uploading_file = self.tmp_dir + '/' + str(offset)
        with open(uploading_file, "wb") as newFile:
            newFile.write(block)
        with open(uploading_file, "rb") as data:
            self.container_client.upload_blob(name = str(offset), data = data)
        os.remove(uploading_file)

    def delete_block(self, offset):
        test = False
        if isinstance(offset, int):
            test = True
        offset = str(offset)
        list_ = self.list_blocks()
        if test == True:
            if int(offset) in list_:
                blob_client = self.container_client.get_blob_client(str(offset))
                blob_client.delete_blob()
        else:
            if offset in list_:
                blob_client = self.container_client.get_blob_client(str(offset))
                blob_client.delete_blob()


##############################################################################




class Google_Cloud_Storage(cloud_storage):
    def __init__(self):
        self.credential_file = "gcp-credential.json"
        self.bucket_name = "csce678-s21-p1-323000675"
        with open('gcp-credential.json') as source:
            self.info = json.load(source)
        self.storage_credentials = service_account.Credentials.from_service_account_info(self.info)
        self.storage_client = storage.Client(credentials = self.storage_credentials)
        self.bucket = self.storage_client.bucket(self.bucket_name)
        self.tmp_dir = 'tmp'

    #TODO
    def list_blocks(self):
        blobs = self.storage_client.list_blobs(self.bucket_name)
        list_ = []
        for blob in blobs:
            list_.append(str(blob.name))
        ret = []
        for b in list_:
            if "_mohammad_" in b:
                ret.append(b)
            else:
                ret.append(int(b)) 
        return ret

    def read_block(self, offset):
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        offset = str(offset)
        downloaded_file = self.tmp_dir + '/' + str(offset) 
        blob = self.bucket.blob(str(offset))
        blob.download_to_filename(downloaded_file)
        if os.path.exists(downloaded_file):
            with open(downloaded_file, "rb") as binaryfile :
                myArr = bytearray(binaryfile.read())
                os.remove(downloaded_file)
                return myArr
        
    def write_block(self, block, offset):
        offset = str(offset)
        if not os.path.exists(self.tmp_dir):
            os.makedirs(self.tmp_dir)
        uploading_file = self.tmp_dir + '/' + str(offset)
        with open(uploading_file, "wb") as newFile:
            newFile.write(block)
        blob = self.bucket.blob(str(offset))
        blob.upload_from_filename(uploading_file)
        os.remove(uploading_file)
        

    def delete_block(self, offset):
        test = False
        if isinstance(offset, int):
            test = True
        offset = str(offset)
        list_ = self.list_blocks()
        if test == True:
            if int(offset) in list_:
                blob = self.bucket.blob(str(offset))
                blob.delete()
        else:
            if offset in list_:
                blob = self.bucket.blob(str(offset))
                blob.delete()


##############################################################################


class RAID_on_Cloud(NAS):
    def __init__(self):
        self.backends = [AWS_S3(), Azure_Blob_Storage(), Google_Cloud_Storage()]
        self.map = dict() # <filename, fid> === <string, int>
        self.highest_available_fid = 0
        self.cloud_provider = 0 #aws = 0; azure = 1; google = 3

    def open(self, filename):
        #convert filename variable in string
        filename = str(filename)
        #get all the file descriptor keys
        filenames = self.map.keys()
        #iterate over all filenames and check if the filename already open
        for f in filenames:
            if f == filename:
                #get the value
                selected_fid = self.map.get(f)
                return int(selected_fid)
        #coming here means its a new fd
        #so get the highest available fid and increment
        chosen_fid = self.highest_available_fid
        self.map[str(filename)] = chosen_fid
        self.highest_available_fid = self.highest_available_fid + 1	
        return chosen_fid
            

    def read(self, fd, length, offset):
        #convert fd into int
        fd = int(fd)
        length = int(length)
        offset = int(offset)
        if int(fd) not in self.map.values():
            dummy = bytearray()
            return dummy
        #get the filename
        filename = ""
        for f in self.map.keys():
            if self.map.get(f) == fd:
                filename = f
        #define a list of all possible blocks for this file
        all_blocks = []
        #get all blocks from aws
        aws = AWS_S3()
        aws_blocks = aws.list_blocks()
        #get all blocks from azure
        azure = Azure_Blob_Storage()
        azure_blocks = azure.list_blocks()
        #get all blocks from google
        google = Google_Cloud_Storage()
        google_blocks = google.list_blocks()
        #check if the file doesnt exists in the cloud at all
        exists_in_cloud = False
        for b in aws_blocks:
            if str(b).split("_mohammad_")[0] == filename:
                exists_in_cloud = True
        for b in azure_blocks:
            if str(b).split("_mohammad_")[0] == filename:
                exists_in_cloud = True
        for b in google_blocks:
            if str(b).split("_mohammad_")[0] == filename:
                exists_in_cloud = True
        if not exists_in_cloud:
            dummy = bytearray()
            return dummy
        #check how many blocks of this file exists in aws
        for block_name in aws_blocks:
            fname = str(block_name).split("_mohammad_")[0]
            if fname == filename:
                all_blocks.append(block_name)
        #check how many blocks of this file exists in azure
        for block_name in azure_blocks:
            fname = str(block_name).split("_mohammad_")[0]
            if fname == filename:
                all_blocks.append(block_name)
        #check how many blocks of this file exists in google
        for block_name in google_blocks:
            fname = str(block_name).split("_mohammad_")[0]
            if fname == filename:
                all_blocks.append(block_name)
        #filter out the common blocks
        #unique_blocks[] contains all blocks with string names and "_mohammad_" tag
        unique_blocks = []
        for block_name in all_blocks:
            if block_name not in unique_blocks:
                unique_blocks.append(block_name)
        #check if all blocks from 0 to n has been downloaded
        block_index = []
        for block_name in unique_blocks:
            block_index.append(int(block_name.split("_mohammad_")[1]))
        count = len(unique_blocks)
        for c in range (0, count):
            if c not in block_index:
                dummy = bytearray()
                return dummy
        #now lets download all blocks and append
        filebytes = bytearray()
        for i in range(0, len(block_index)):
            block_name = filename + "_mohammad_" + str(i)
            if block_name in aws_blocks:
                block = aws.read_block(block_name)
                filebytes = filebytes + block
            elif block_name in azure_blocks:
                block = azure.read_block(block_name)
                filebytes = filebytes + block
            elif block_name in google_blocks:
                block = google.read_block(block_name)
                filebytes = filebytes + block
        #at this point, filebytes[] has the entire file
        #check if user wants to read the entire file (length = -1, offset = -1)
        if length==-1 and offset ==-1:
            return filebytes
        #check if the offset if within the bytearray or beyond
        if int(offset) > len(filebytes):
            dummy = bytearray()
            return dummy
        else:
            #check if the length from offset exceeds the filebytes length
            if (offset+length) > len(filebytes):
                res = bytearray(len(filebytes) - offset)
                ind = 0
                for i in range (offset, len(filebytes)):
                    res[ind] = filebytes[i]
                    ind = ind + 1
                return res
            else:
                #simply get the length amount of bytes from filebytes only
                res = bytearray(length)
                ind = 0
                for i in range (offset, (offset+length)):
                    res[ind] = filebytes[i]
                    ind = ind + 1
                return res
                        
        
           

    def write(self, fd, data, offset):
        #convert fd into int
        #check if the file even open aka has a fid
        if fd not in self.map.values():
            raise IOError("File descriptor %d does not exist." % int(fd))
        #get the filename
        filename = ""
        for fname in self.map.keys():
            if self.map.get(fname) == fd:
                filename = fname
        #get all blocks from aws
        aws = AWS_S3()
        aws_blocks = aws.list_blocks()
        #get all blocks from azure
        azure = Azure_Blob_Storage()
        azure_blocks = azure.list_blocks()
        #get all blocks from google
        google = Google_Cloud_Storage()
        google_blocks = google.list_blocks()
        #define entire_file here
        entire_file  = bytearray()
        #check if the file doesnt exists in the cloud at all
        exists_in_cloud = False 
        for b in aws_blocks:
            if (type(b)==str) and str(b).split("_mohammad_")[0] == filename:
                exists_in_cloud = True
        for b in azure_blocks:
            if(type(b)==str) and str(b).split("_mohammad_")[0] == filename:
                exists_in_cloud = True
        for b in google_blocks:
            if(type(b)==str) and  str(b).split("_mohammad_")[0] == filename:
                exists_in_cloud = True
        if not exists_in_cloud:
            #this is a new file and cloud does not have any data for this file.
            #ensure that entire_file contains [0 to offset] + [offset to len(data)] amounf of length
            entire_file = bytearray(offset + len(data))
            for i in range (0, offset):
                entire_file[i] = 0
            offset_ = offset
            for i in range (0, len(data)):
                entire_file[offset_] = data[i]
                offset_ = offset_ + 1
        else:
            #this file has previous data blocks in the cloud so download the original file as bytearray first
            entire_file = self.read(fd, -1, -1)
            #delete this file from cloud
            self.delete(filename)
            #put the filenam-fd into map
            self.map[filename] = fd
            #decide offset is outside of file length of entire_file or inside 
            if offset > len(entire_file):
                #get how many bytes from eof to offset
                zeros = bytearray(offset - len(entire_file))
                for i in range(0, len(zeros)):
                    zeros[i] = 0
                #corner case: check if zeros[] len is 0, in that case dont add with entire_file
                if (offset - len(entire_file))!=0:
                    entire_file = entire_file + zeros
                #now append the input block(data) at the end of entire_file
                entire_file = entire_file + data
            else:
                #the offset is inside the length of entire_file
                #if data cannot fit within current entire_file
                if (offset + len(data)) > len(entire_file):
                    #increase entire_file length
                    entire_file  = entire_file + bytearray(offset + len(data) - len(entire_file))
                #apply the changes on entire_file
                offset_= offset
                for i in range(0, len(data)):
                    entire_file[offset_] = data[i]    
                    offset_ = offset_ + 1
        #at this point entire_file[] is ready to be distributed                 
        #now we need to divide them block by block and push
        block_size = cloud_storage.block_size
        if(block_size > len(entire_file)):
            block_size = len(entire_file)
        start = 0
        end = block_size
        block_count = 0
        while end <= len(entire_file):
            block = bytearray(end-start)
            ind = 0
            for i in range (start, end):
                block[ind] = entire_file[i]
                ind = ind + 1
            if self.cloud_provider == 0:
                aws.write_block(block, str(filename + "_mohammad_" + str(block_count)))
                azure.write_block(block, str(filename + "_mohammad_" + str(block_count)))
                self.cloud_provider = 1
            elif self.cloud_provider == 1:
                azure.write_block(block, str(filename + "_mohammad_" + str(block_count)))
                google.write_block(block, str(filename + "_mohammad_" + str(block_count)))
                self.cloud_provider = 2
            elif self.cloud_provider == 2:
                google.write_block(block, str(filename + "_mohammad_" + str(block_count)))
                aws.write_block(block, str(filename + "_mohammad_" + str(block_count)))
                self.cloud_provider = 0
            block_count = block_count + 1
            start = end
            if end == len(entire_file):
                break
            if end+block_size > len(entire_file):
                end = len(entire_file)
            else:
                end = end + block_size
           

          
    def close(self, fd):
        if isinstance(fd, int):
            fd = int(fd)
        if isinstance(fd, str):
            fd = int(fd)
        #get all fids
        fids = self.map.values()
        if fd not in fids:
            raise IOError("File descriptor %d does not exist." % int(fd))
        else:
            filenames = self.map.keys()
            for f in filenames:
                if self.map.get(f)  == fd:
                    self.map.pop(f)
                    break
        

    def delete(self, filename):
        filename = str(filename)
        #download a list from AWS
        aws = AWS_S3()
        list_aws = aws.list_blocks()
        #download a list from Azure
        azure = Azure_Blob_Storage()
        list_azure = azure.list_blocks()
        #download a list from google
        google = Google_Cloud_Storage()
        list_google = google.list_blocks()
        #delete any file that has filename as initial from aws
        file_exists = False
        for block_name in list_aws:
            fname = str(block_name).split("_mohammad_")[0]
            if fname == filename:
                aws.delete_block(str(block_name))
                file_exists = True
        #delete any file that has filename as initial from azure
        for block_name in list_azure:
            fname = str(block_name).split("_mohammad_")[0]
            if fname == filename:
                azure.delete_block(str(block_name))
                file_exists = True
        #delete any file that has filename as initial from google
        for block_name in list_google:
            fname = str(block_name).split("_mohammad_")[0]
            if fname == filename:
                google.delete_block(str(block_name))
                file_exists = True
        if file_exists == False:
            raise IOError("No such file or directory.")
        #remove the filename from the map, hence close the file descriptor
        if (filename in self.map.keys()):
            self.map.pop(filename)
         
        

    def get_storage_sizes(self):
        return [len(b.list_blocks()) for b in self.backends]
    
    #This function decides where should we store the next block
    def decideBalance(self, cloud):
        print "This function decides where should we store the next block"
 
    def listMap(self):
        print self.map
        

##############################################################################

##############################################################################

