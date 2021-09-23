

import pandas as pd
import numpy as np
import hashlib
import os


def get_col_list(tbl_lst:list, PII_suffix:list, PII_str_lst:list, exclusion_lst:list=None)->list:
    '''
    Creates list of columns which require annonymisation
    Args:
    tbl_lst : list of tables that need to be anonymised
    suffix : list of suffix which could indicate columns with these suffix in thier names may be PII columns
    exclusion_lst = list of column_names which should be excluded from anonymisation
    '''
    col_lst = list(set([col for tbl in tbl_lst for col in tbl.columns if any([col.lower().endswith(tuple(s.lower() for s in PII_suffix)), *[c for c in PII_str_lst if c in(col)]])]))
    col_lst = [col for col in col_lst if col.lower() not in [ex.lower() for ex in exclusion_lst]] if exclusion_lst else col_lst
    return col_lst
  
def convert_to_str(tbl_lst:list, col_lst:list)->list:
    for col in col_lst:
        for tbl in tbl_lst:
            if col in tbl.columns:
                tbl[col] = tbl[col].apply(str)
    return tbl_lst
  
def anonymise_tbl(tbl_lst:list, col_lst:list, salt:str='random')->tuple:
    salt = salt.encode() if salt != 'random' else os.urandom(32)
    for col in col_lst:
        nested_unique_value_lst = [tbl[col].unique().tolist() for tbl in tbl_lst if col in tbl.columns]
        unique_value_lst = list(set([unique_value for unique_value_lst in nested_unique_value_lst for unique_value in unique_value_lst]))
        hash_lst = [hashlib.sha256(unique_value.encode()+salt).hexdigest() for unique_value in unique_value_lst]
        hash_map = dict(zip(unique_value_lst, hash_lst))
        for tbl in tbl_lst:
            try:
                tbl[col] = tbl[col].map(hash_map)
            except KeyError :
                pass
    return tuple(tbl_lst)
    


# # File locations
arrearsData = "/dbfs/FileStore/tables/Dee/Data/Damp_&_Mould/arrearsData.csv"
mainReqData = "/dbfs/FileStore/tables/Dee/Data/Damp_&_Mould/MainReqRJoinWorkOrder.csv"
occupantsData = "/dbfs/FileStore/tables/Dee/Data/Damp_&_Mould/UnitsTenancyTenantsOccupants.csv"
tenancyDurationData = "/dbfs/FileStore/tables/Dee/Data/Damp_&_Mould/tenancyDurationResults.csv"
keyStone2Data = "/dbfs/FileStore/tables/Dee/Data/Damp_&_Mould/keyStoneEpc2.csv"


arrears = pd.read_csv(arrearsData)
unitOccupants = pd.read_csv(occupantsData)
duration = pd.read_csv(tenancyDurationData)
epc = pd.read_csv(keyStone2Data)
workOrder = pd.read_csv(mainReqData)



# # File locations
arrearsData = "/dbfs/FileStore/tables/Dee/Data/Damp_&_Mould/arrearsData.csv"
mainReqData = "/dbfs/FileStore/tables/Dee/Data/Damp_&_Mould/MainReqRJoinWorkOrder.csv"
occupantsData = "/dbfs/FileStore/tables/Dee/Data/Damp_&_Mould/UnitsTenancyTenantsOccupants.csv"
tenancyDurationData = "/dbfs/FileStore/tables/Dee/Data/Damp_&_Mould/tenancyDurationResults.csv"
keyStone2Data = "/dbfs/FileStore/tables/Dee/Data/Damp_&_Mould/keyStoneEpc2.csv"


arrears = pd.read_csv(arrearsData)
unitOccupants = pd.read_csv(occupantsData)
duration = pd.read_csv(tenancyDurationData)
epc = pd.read_csv(keyStone2Data)
workOrder = pd.read_csv(mainReqData)



arrears_sdf = spark.createDataFrame(arrears)
unitOccupants_sdf = spark.createDataFrame(unitOccupants)
duration_sdf = spark.createDataFrame(duration)
epc_sdf = spark.createDataFrame(epc)
workOrder_sdf = spark.createDataFrame(workOrder)

arrears_sdf\
  .coalesce(1).write.format("csv")\
  .option("header", "true")\
  .mode("overwrite")\
  .save("FileStore/tables/Dee/DnM_Anonymised/arrears")

unitOccupants_sdf\
  .coalesce(1).write.format("csv")\
  .option("header", "true")\
  .mode("overwrite")\
  .save("FileStore/tables/Dee/DnM_Anonymised/unitOccupants")

duration_sdf\
  .coalesce(1).write.format("csv")\
  .option("header", "true")\
  .mode("overwrite")\
  .save("FileStore/tables/Dee/DnM_Anonymised/duration")

epc_sdf\
  .coalesce(1).write.format("csv")\
  .option("header", "true")\
  .mode("overwrite")\
  .save("FileStore/tables/Dee/DnM_Anonymised/epc")

workOrder_sdf\
  .coalesce(1).write.format("csv")\
  .option("header", "true")\
  .mode("overwrite")\
  .save("FileStore/tables/Dee/DnM_Anonymised/workOrder")
