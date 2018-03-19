# This file is to divide large size csv data into several parts
# load required pkg
import csv
import pandas as pd
import codecs


# set desired params
chunk_num = 5

# Below there are 3 instances that require the settings of path

# read the data file (csv)
with open('D:/dropbox/Dropbox/InProgress/Data/AidDataCoreFull_ResearchRelease_Level1_v3.1.csv',  encoding='utf8') as g:
    csv_data = csv.reader(g, delimiter=',' , quotechar='"', )
    # save the headings
    heading =  next(csv_data)
    # count the rows
    row_count = len(g.readlines())
    mod_rowcount = row_count % chunk_num # the 5 is the desired number of chunks for the bulk data
    cksize =  row_count // chunk_num
 
    

# create containers for each chunk
createvar = locals()
for i in range(chunk_num):
    createvar['data_chunk'+ str(i)] = pd.DataFrame()
# Via pandas, it's easier to operate the big dataset
j = 0 
pd_csv = pd.read_csv('D:/dropbox/Dropbox/InProgress/Data/AidDataCoreFull_ResearchRelease_Level1_v3.1.csv', iterator = True, chunksize = cksize + 1, dtype = h_dtype)
for row in pd_csv:
    createvar['data_chunk'+ str(j)] = createvar['data_chunk'+ str(j)].append(row)
    j = j + 1
    
# write them to each csv
       
for k in range(chunk_num):
    path_chunk = 'D:/dropbox/Dropbox/InProgress/Data/' + 'aid' +str(k)
    createvar['data_chunk'+ str(i)].to_csv(path_chunk, header = True)



    
