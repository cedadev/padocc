__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2023 United Kingdom Research and Innovation"

#from ingest_lib import Ingester
import glob
import json

# Code to handle indexing detail-cfg files from the pipeline into a STAC-like Elasticsearch index of created Kerchunk files

# NOTE: Ingestion and indexing are final steps of the pipeline (post-validation),
# but detail-cfg could be uploaded during the pipeline, with the caveat that a setting be required to show pipeline status
# but this would mean multiple updates?

# Any Elasticsearch index should also include the group name (possibly part of each record?) for search purposes.

class STACIndexer:
    pass

class IngestOperation:
    pass

def add_download_link(group, workdir, proj_code):
        """
        Add the download link to each of the Kerchunk references
        """
        complete_kerchunk = f'{workdir}/complete/{group}/{proj_code}*'
        kerchunks = glob.glob(complete_kerchunk)
        if len(kerchunks) == 0:
            print('No complete kerchunk file found - deal with this somehow')
            raise NotImplementedError
        elif len(kerchunks) > 1:
            print('More than one kerchunk file specified, version number required')
            raise NotImplementedError
        else:
            kfile = kerchunks[0]
            with open(kfile) as f:
                refs = json.load(f)

            for key in refs.keys():
                if len(refs[key]) == 3:
                    if refs[key][0][0] == '/':
                        refs[key][0] = 'https://dap.ceda.ac.uk' + refs[key][0]

            with open(kfile,'w') as f:
                f.write(json.dumps(refs))
            
def ingest_config(args, logger):
    """
    Configure for ingestion of a set of project codes, currently defined
    by a repeat_id but this could be changed later to apply to all project
    codes fitting some parameters"""

    proj_codes = get_codes(args.groupID, args.workdir, f'proj_codes/{args.repeat_id}')

    for p in proj_codes:
        proj_dir = get_proj_dir(p, args.workdir, args.groupID)
        detail = get_proj_file(proj_dir, 'detail-cfg.json')

        # Any old files which don't have this parameter must by definition already 
        # have the links required.
        add_links = True
        if 'links_added' in detail:
            if detail['links_added']:
                add_links = False
        else:
            add_links = False

        if add_links:
            add_download_link(args.groupID, args.workdir, p)