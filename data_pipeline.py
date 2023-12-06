import requests
import xmltodict
import pandas as pd
import os
import logging
import numpy as np
from datetime import datetime

logging.basicConfig(filename='/data/data_pipeline.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

log = logging.getLogger()

def get_bods_data(bb_min_lon:float, bb_min_lat:float, bb_max_lon:float, 
                  bb_max_lat:float):

    #boundingBox format: [minLongitude, minLatitude, maxLongitude, maxLatitude]
    url = 'https://data.bus-data.dft.gov.uk/api/v1/datafeed'
    params = {'boundingBox': "{:.4f},{:.4f},{:.4f},{:.4f}".format(bb_min_lon,
                                                                  bb_min_lat, 
                                                                  bb_max_lon, 
                                                                  bb_max_lat), 
              'api_key': os.environ['API_Key']}
    
    retrieval_time = datetime.now()
    log.info(f"Sending request at time: \
             {retrieval_time.strftime('%Y-%m-%d %H:%M:%S')}")
    r = requests.get(url, params=params)
    
    if r.status_code == 200:
        log.info('Request successful')
        try:
            doc = xmltodict.parse(r.content)
            # Filter to the relevant xml item
            doc = doc['Siri']['ServiceDelivery']['VehicleMonitoringDelivery']['VehicleActivity']
        except:
            log.error('Error in parsing xml data to dataframe')
            return
        
        columns = ['RecordedAtTime','ItemIdentifier','ValidUntilTime','Extensions','MonitoredVehicleJourney.LineRef',
                   'MonitoredVehicleJourney.DirectionRef','MonitoredVehicleJourney.PublishedLineName',
                   'MonitoredVehicleJourney.OperatorRef','MonitoredVehicleJourney.OriginRef',
                   'MonitoredVehicleJourney.OriginName','MonitoredVehicleJourney.DestinationRef',
                   'MonitoredVehicleJourney.DestinationName','MonitoredVehicleJourney.OriginAimedDepartureTime',
                   'MonitoredVehicleJourney.VehicleLocation.Longitude',
                   'MonitoredVehicleJourney.VehicleLocation.Latitude','MonitoredVehicleJourney.VehicleJourneyRef',
                   'MonitoredVehicleJourney.VehicleRef','MonitoredVehicleJourney.Bearing',
                   'MonitoredVehicleJourney.FramedVehicleJourneyRef.DataFrameRef',
                   'MonitoredVehicleJourney.FramedVehicleJourneyRef.DatedVehicleJourneyRef',
                   'MonitoredVehicleJourney.BlockRef',
                   'MonitoredVehicleJourney.DestinationAimedArrivalTime',
                   'Extensions.VehicleJourney.DriverRef',
                   'Extensions.VehicleJourney.Operational.TicketMachine.TicketMachineServiceCode',
                   'Extensions.VehicleJourney.Operational.TicketMachine.JourneyCode',
                   'Extensions.VehicleJourney.VehicleUniqueId']
        
        df = pd.DataFrame(columns=columns)
        df = pd.concat([df, pd.json_normalize(doc)])
        
        # Convert datetime fields
        df['RecordedAtTime'] = pd.to_datetime(df['RecordedAtTime'])
        df['ValidUntilTime'] = pd.to_datetime(df['ValidUntilTime'])
        df['MonitoredVehicleJourney.OriginAimedDepartureTime'] = pd.to_datetime(df['MonitoredVehicleJourney.OriginAimedDepartureTime'])
        df['MonitoredVehicleJourney.DestinationAimedArrivalTime'] = pd.to_datetime(df['MonitoredVehicleJourney.DestinationAimedArrivalTime'])

        # Convert numeric fields
        df['MonitoredVehicleJourney.VehicleLocation.Longitude'] = pd.to_numeric(df['MonitoredVehicleJourney.VehicleLocation.Longitude'])
        df['MonitoredVehicleJourney.VehicleLocation.Latitude'] = pd.to_numeric(df['MonitoredVehicleJourney.VehicleLocation.Latitude'])
        df['MonitoredVehicleJourney.Bearing'] = pd.to_numeric(df['MonitoredVehicleJourney.Bearing'])
        df['MonitoredVehicleJourney.VehicleJourneyRef'] = pd.to_numeric(df['MonitoredVehicleJourney.VehicleJourneyRef'])

        # Set retrieval date
        df['RetrievalDate'] = pd.to_datetime(retrieval_time).tz_localize('UTC')
        
        # Filter to only recent records
        df = df[np.abs(df['RetrievalDate'] - df['RecordedAtTime']).dt.total_seconds() < 300]

    else:
        log.error(f'Response status code: {r.status_code}')
        log.error(f'Response content: {r.content}')

        return

    return df, retrieval_time


if __name__=="__main__":

    log.info('Running data pipeline')
    
    bb_min_lon = -0.260707
    bb_min_lat = 51.412938
    bb_max_lon = 0.128712
    bb_max_lat = 51.574489

    df, retrieval_time = get_bods_data(bb_min_lon, bb_min_lat, bb_max_lon, bb_max_lat)

    filename = f"/data/BODS-BoundingBox{bb_min_lon}_{bb_min_lat}_{bb_max_lon}_{bb_max_lat}-RetrievalTime{retrieval_time.strftime('%Y%m%d%H%M%S')}.parquet"

    df.to_parquet(filename, index=False)
