import pandas as pd
import json
import numpy as np
import os
import sys
import manifest


import sys, os, json, collections, struct, datetime


def convert_txt_to_bin(filename, outfilename, mig_type, id_reference):

    max_destinations_per_node = 1
    destinations_per_node = 0

    fopen=open(filename)
    fout=open(outfilename,'wb')
    net={}
    net_rate={}

    node_id_list = []
    prev_id = -1
    for line in fopen:
        s=line.strip().split(',')
        ID1=int(float(s[0]))
        ID2=int(float(s[1]))
        rate=float(s[2])
        if ID1 not in net:
            net[ID1]=[]
            net_rate[ID1]=[]
        net[ID1].append(ID2)
        net_rate[ID1].append(rate)
        if prev_id != ID1:
            if( destinations_per_node > max_destinations_per_node ):
                max_destinations_per_node = destinations_per_node
            node_id_list.append(ID1)
            # print(prev_id, max_destinations_per_node)
            prev_id = ID1
            destinations_per_node = 0
        destinations_per_node += 1


    for ID in net:
        ID_write=[]
        ID_rate_write=[]
        for i in range(max_destinations_per_node):
            ID_write.append(0)
            ID_rate_write.append(0)
        for i in range(len(net[ID])):
            ID_write[i]=net[ID][i]
            ID_rate_write[i]=net_rate[ID][i]
        s_write=struct.pack('I'*len(ID_write), *ID_write)
        s_rate_write=struct.pack('d'*len(ID_rate_write),*ID_rate_write)
        fout.write(s_write)
        fout.write(s_rate_write)

    fopen.close()
    fout.close()

    offset_str = ""
    nodecount = 0

    for ID in net:
        offset_str += '%0.8X' % ID
        offset_str += '%0.8X' % (nodecount * max_destinations_per_node * 12) # 12 -> sizeof(uint32_t) + sizeof(double)
        nodecount += 1


    migjson = collections.OrderedDict([])
    migjson['Metadata'] = {}
    migjson['Metadata']['Author'] = "tmh6260"
    migjson['Metadata']['NodeCount'] = len(node_id_list)
    migjson['Metadata']['IdReference'] = id_reference
    migjson['Metadata']['DateCreated'] = datetime.datetime.now().ctime()
    migjson['Metadata']['Tool'] = os.path.basename(sys.argv[0])
    migjson['Metadata']['DatavalueCount'] = max_destinations_per_node
    migjson['Metadata']['MigrationType'] = mig_type
    migjson['NodeOffsets'] = offset_str

    with open(outfilename+".json", 'w') as file:
        json.dump(migjson, file, indent=4)


if __name__ == '__main__' :

    outfile_base=os.path.join('.','simulation_inputs', 'migration','migration_test')
    id_reference = 'FE_EXAMPLE'
    mig_type = 'Local'
    convert_txt_to_bin(os.path.join(outfile_base,'%s_Migration.csv' % (mig_type)),
                       os.path.join(outfile_base,'%s_Migration.bin' % (mig_type)),
                       mig_type='%s_MIGRATION' % mig_type.upper(),
                       id_reference=id_reference)
