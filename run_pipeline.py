import os
from data_pipeline import get_bods_data

bb_min_lon = os.environ['BB_MIN_LON']
bb_min_lat = os.environ['BB_MIN_LAT']
bb_max_lon = os.environ['BB_MAX_LON']
bb_max_lat = os.environ['BB_MAX_LAT']

df, retrieval_time = get_bods_data(bb_min_lon, bb_min_lat, bb_max_lon, bb_max_lat)

filename = f"/data/BODS-BoundingBox{bb_min_lon}_{bb_min_lat}_{bb_max_lon}_{bb_max_lat}-RetrievalTime{retrieval_time.strftime('%Y%m%d%H%M%S')}.parquet"

df.to_parquet(filename, index=False)