import os, sys
import pandas as pd
import glob

KERCHUNK_INTAKE = '/home/users/dwest77/Documents/intake_dev/kerchunk-intake'

def add_update_entry(dataframe, entryarr, colset):
    # Dataframe - main pandas dataframe
    # Entryarr  - array of row-like items for this entry
    # colset    - pairs of column name to index for the matchmaking
    #             i.e overwrite when matching 1st 2nd and 4th columns
    queries = []
    for column, index in colset:
        value = entryarr[index]
        queries.append(f"{column} == '{value}'")

    query = ' and '.join(queries)
    result = dataframe.query(query)
    if not result.empty:
        dataframe = dataframe.drop(result.index)
        add_ind = result.index[0]
    else:
        add_ind = len(dataframe)

    # Add entry
    dataframe.loc[add_ind] = entryarr
    return dataframe

def update_table(dataframe, new_data, columns):
    # Assemble the colset match parameters
    colset = [(c, x) for x, c in enumerate(columns[:9])]

    # For each entry, add or update entries in the existing dataframes by matches
    for entryid in range(len(new_data[columns[0]])):
        entryarr = []
        for c in columns:
            entryarr.append(new_data[c][entryid])
        dataframe = add_update_entry(dataframe, entryarr, colset)
    return dataframe

def check_put(bucket_id, path, xfile, clobber=False):
    if clobber or xfile not in [obj.object_name for obj in mc.list_objects(bucket_id, recursive=recursive)]:
        mc.fput_object(bucket_id, path, file)

def assemble_filelist(jobid):
    # Find all log files with this jobid
    joblogs = glob.glob(f'{KERCHUNK_INTAKE}/outs/{jobid}_*.out')
    jobfiles = []
    maincsv = ''
    for job in joblogs:
        f = open(job,'r')
        maincsv, jobfile = f.readlines()[-2:]
        f.close()
        jobfiles.append(jobfile.replace('\n',''))
    return jobfiles, maincsv

def get_columns():
    g = open(f'{KERCHUNK_INTAKE}/templates/columns.txt','r')
    columns = [l.replace('\n','') for l in g.readlines()]
    columns = columns[:12]
    g.close()
    return columns

def main(args):

    jobid = args[1]
    print(f'[INFO] Starting Assembly for {jobid}')

    jobfiles, maincsv = assemble_filelist(jobid)
    columns           = get_columns()

    if len(jobfiles) < 1:
        print(f'Error: No job files found with {jobid} id - check under /outs')
        return None

    push_files = []
    try:
        s3_push = sys.argv[2] =='True'
    except:
        s3_push = False

    new_data = {c:[] for c in columns}
    for jobfile in jobfiles:
        f = open(jobfile,'r')
        content = f.readlines()
        f.close()

        for line in content:
            line = line.replace('\n','')
            for x, part in enumerate(line.split(',')):
                new_data[columns[x]].append(part)
            if s3_push:
                kfile = new_data['uri'][-1]
                s3kfile = f's3://{bucket_id}/{os.path.basename(kfile)}'
                push_files.append(kfile)
                new_data['uri'][-1] = s3kfile
    local_csv  = maincsv.replace('\n','')
    local_json = local_csv.replace('.csv','.json')

    ## Stage 1: Assemble local csv from s3 if appropriate
    if s3_push:
        if s3_file_exist(os.path.basename(local_csv), bucket_id):
            pass

            # Requires s3 access to test but this is the gist
            #mc.get_object()
            #dataframe = pd.from_csv(local_csv)

    ## Stage 2: Add new data to frame with contitional dropping and checks
    if os.path.isfile(local_csv):
        current_frame = pd.read_csv(local_csv)
        new_frame = update_table(current_frame, new_data, columns)
    else:
        new_frame = pd.DataFrame(data=new_data)
    new_frame.to_csv(local_csv, mode='w', header=True, index=False)

    ## Stage 3: Push json, csv, kfiles to s3
    if s3_push:
        check_put(bucket_id, os.path.basename(local_csv), local_csv)

        check_put(bucket_id, os.path.basename(local_json), local_json)

        for kfile in push_files:
            check_put(bucket_id, os.path.basename(kfile), kfile)

    print('[INFO] Assembly complete')

# Extract all final lines csv files into a single list
# Extract a single one of the catalog files
# Open each csv file in turn and build a dataframe
# Open the main file and add the dataframe
# End job

main(sys.argv)