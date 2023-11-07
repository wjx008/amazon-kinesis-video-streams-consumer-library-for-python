# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0.

'''
Example to demonstrate usage the AWS Kinesis Video Streams (KVS) Consumer Library for Python.
 '''
 
__version__ = "0.0.1"
__status__ = "Development"
__copyright__ = "Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved."
__author__ = "Dean Colcott <https://www.linkedin.com/in/deancolcott/>"


import os
from io import BytesIO
import sys
import time
import random
import math
import json
import string

import boto3
import cv2
import pandas as pd
from ultralytics import YOLO
import logging
from amazon_kinesis_video_consumer_library.kinesis_video_streams_parser import KvsConsumerLibrary
from amazon_kinesis_video_consumer_library.kinesis_video_fragment_processor import KvsFragementProcessor
from sort.sort import *


def random_string(N=20):
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(N))


def convert_to_dynamodb_item(row):
    item = {}
    for column, value in row.items():
        item[column] = str(value)  # Convert values to strings as needed
    return item


def get_car(license_plate, vehicles):

    x1, y1, x2, y2  = license_plate

    foundIt = False
    for j in range(len(vehicles)):
        xcar1, ycar1, xcar2, ycar2, score, class_id = vehicles[j]

        if x1 > xcar1 and y1 > ycar1 and x2 < xcar2 and y2 < ycar2:
            car_indx = j
            foundIt = True
            break

    if foundIt:
        return xcar1, ycar1, xcar2, ycar2

    return -1, -1, -1, -1



# Config the logger.
log = logging.getLogger(__name__)
logging.basicConfig(format="[%(name)s.%(funcName)s():%(lineno)d] - [%(levelname)s] - %(message)s", 
                    stream=sys.stdout, 
                    level=logging.INFO)

# Update the desired region and KVS stream name.
REGION='region-name'
KVS_STREAM01_NAME = 'stream-name'   # Stream must be in specified region

license_plate_detector = YOLO('../car_and_license_plate_detector.pt')

s3_client = boto3.client('s3', region_name=REGION)
dynamodb_client = boto3.resource('dynamodb', region_name=REGION)
sqs_client = boto3.client('sqs', region_name=REGION)

bucket_name = 'bucket-name'

table_name = 'table-name'

queue_url = 'queue-url'

processed_track_ids = []

data = pd.DataFrame(columns=['fragment_number', 'abs_frame_number', 'frame_number', 'x1', 'y1', 'x2', 'y2', 'x1car', 'y1car', 'x2car', 'y2car', 'class_id', 'track_id'])

counter = 0

fragment_nmrs = []

mot_tracker = Sort()
class KvsPythonConsumerExample:
    '''
    Example class to demonstrate usage the AWS Kinesis Video Streams KVS) Consumer Library for Python.
    '''

    def __init__(self):
        '''
        Initialize the KVS clients as needed. The KVS Comsumer Library intentionally does not abstract 
        the KVS clients or the various media API calls. These have individual authentication configuration and 
        a variety of other user defined settings so we keep them here in the users application logic for configurability.

        The KvsConsumerLibrary sits above these and parses responses from GetMedia and GetMediaForFragmentList 
        into MKV fragments and provides convenience functions to further process, save and extract individual frames.  
        '''

        # Create shared instance of KvsFragementProcessor
        self.kvs_fragment_processor = KvsFragementProcessor()

        # Variable to maintaun state of last good fragememt mostly for error and exception handling.
        self.last_good_fragment_tags = None

        # Init the KVS Service Client and get the accounts KVS service endpoint
        log.info('Initializing Amazon Kinesis Video client....')
        # Attach session specific configuration (such as the authentication pattern)
        self.session = boto3.Session(region_name=REGION)
        self.kvs_client = self.session.client("kinesisvideo")

    ####################################################
    # Main process loop
    def service_loop(self):
        
        ####################################################
        # Start an instance of the KvsConsumerLibrary reading in a Kinesis Video Stream

        # Get the KVS Endpoint for the GetMedia Call for this stream
        log.info(f'Getting KVS GetMedia Endpoint for stream: {KVS_STREAM01_NAME} ........') 
        get_media_endpoint = self._get_data_endpoint(KVS_STREAM01_NAME, 'GET_MEDIA')
        
        # Get the KVS Media client for the GetMedia API call
        log.info(f'Initializing KVS Media client for stream: {KVS_STREAM01_NAME}........') 
        kvs_media_client = self.session.client('kinesis-video-media', endpoint_url=get_media_endpoint)

        # Make a KVS GetMedia API call with the desired KVS stream and StartSelector type and time bounding.
        log.info(f'Requesting KVS GetMedia Response for stream: {KVS_STREAM01_NAME}........') 
        get_media_response = kvs_media_client.get_media(
            StreamName=KVS_STREAM01_NAME,
            StartSelector={
                'StartSelectorType': 'NOW'
            }
        )

        # Initialize an instance of the KvsConsumerLibrary, provide the GetMedia response and the required call-backs
        log.info(f'Starting KvsConsumerLibrary for stream: {KVS_STREAM01_NAME}........') 
        my_stream01_consumer = KvsConsumerLibrary(KVS_STREAM01_NAME, 
                                              get_media_response, 
                                              self.on_fragment_arrived, 
                                              self.on_stream_read_complete, 
                                              self.on_stream_read_exception
                                            )

        # Start the instance of KvsConsumerLibrary, any matching fragments will begin arriving in the on_fragment_arrived callback
        my_stream01_consumer.start()

        # Can create another instance of KvsConsumerLibrary on a different media stream or continue on to other application logic. 

        # Here can hold the process up by waiting for the KvsConsumerLibrary thread to finish (may never finish for live streaming fragments)
        #my_stream01_consumer.join()

        # Or 
    
        # Run a loop with the applications main functionality that holds the process open.
        # Can also use to monitor the completion of the KvsConsumerLibrary instance and trigger a required action on completion.
        while True:

            #Add Main process / application logic here while KvsConsumerLibrary instance runs as a thread
            log.info("Nothn to see, just doin main application stuff in a loop here!")
            time.sleep(5)
            
            # Call below to exit the streaming get_media() thread gracefully before reaching end of stream. 
            #my_stream01_consumer.stop_thread()


    ####################################################
    # KVS Consumer Library call-backs

    def on_fragment_arrived(self, stream_name, fragment_bytes, fragment_dom, fragment_receive_duration):
        global processed_track_ids
        global counter
        global data
        global fragment_nmrs
        '''
        This is the callback for the KvsConsumerLibrary to send MKV fragments as they are received from a stream being processed.
        The KvsConsumerLibrary returns the received fragment as raw bytes and a DOM like structure containing the fragments meta data.

        With these parameters you can do a variety of post-processing including saving the fragment as a standalone MKV file
        to local disk, request individual frames as a numpy.ndarray for data science applications or as JPEG/PNG files to save to disk 
        or pass to computer vison solutions. Finally, you can also use the Fragment DOM to access Meta-Data such as the MKV tags as well
        as track ID and codec information. 

        In the below example we provide a demonstration of all of these described functions.

        ### Parameters:

            **stream_name**: str
                Name of the stream as set when the KvsConsumerLibrary thread triggering this callback was initiated.
                Use this to identify a fragment when multiple streams are read from different instances of KvsConsumerLibrary to this callback.

            **fragment_bytes**: bytearray
                A ByteArray with raw bytes from exactly one fragment. Can be save or processed to access individual frames

            **fragment_dom**: mkv_fragment_doc: ebmlite.core.Document <ebmlite.core.MatroskaDocument>
                A DOM like structure of the parsed fragment providing searchable list of EBML elements and MetaData in the Fragment

            **fragment_receive_duration**: float
                The time in seconds that the fragment took for the streaming data to be received and processed. 
        
        '''
        
        try:
            # Log the arrival of a fragment. 
            # use stream_name to identify fragments where multiple instances of the KvsConsumerLibrary are running on different streams.
            log.info(f'\n\n##########################\nFragment Received on Stream: {stream_name}\n##########################')
            
            # Print the fragment receive and processing duration as measured by the KvsConsumerLibrary
            log.info('')
            log.info(f'####### Fragment Receive and Processing Duration: {fragment_receive_duration} Secs')

            # Get the fragment tags and save in local parameter.
            self.last_good_fragment_tags = self.kvs_fragment_processor.get_fragment_tags(fragment_dom)

            ##### Log Time Deltas:  local time Vs fragment SERVER and PRODUCER Timestamp:
            time_now = time.time()
            kvs_ms_behind_live = float(self.last_good_fragment_tags['AWS_KINESISVIDEO_MILLIS_BEHIND_NOW'])
            producer_timestamp = float(self.last_good_fragment_tags['AWS_KINESISVIDEO_PRODUCER_TIMESTAMP'])
            server_timestamp = float(self.last_good_fragment_tags['AWS_KINESISVIDEO_SERVER_TIMESTAMP'])
            
            log.info('')
            log.info('####### Timestamps and Delta: ')
            log.info(f'KVS Reported Time Behind Live {kvs_ms_behind_live} mS')
            log.info(f'Local Time Diff to Fragment Producer Timestamp: {round(((time_now - producer_timestamp)*1000), 3)} mS')
            log.info(f'Local Time Diff to Fragment Server Timestamp: {round(((time_now - server_timestamp)*1000), 3)} mS')

            ###########################################
            # 1) Extract and print the MKV Tags in the fragment
            ###########################################
            # Get the fragment MKV Tags (Meta-Data). KVS allows these to be set per fragment to convey some information 
            # about the attached frames such as location or Computer Vision labels. Here we just log them!
            log.info('')
            log.info('####### Fragment MKV Tags:')
            for key, value in self.last_good_fragment_tags.items():
                log.info(f'{key} : {value}')

            ###########################################
            # 2) Pretty Print the entire fragment DOM structure
            # ###########################################
            # Get and log the the pretty print string for entire fragment DOM structure from EBMLite parsing.
            log.info('')
            log.info('####### Pretty Print Fragment DOM: #######')
            pretty_frag_dom = self.kvs_fragment_processor.get_fragement_dom_pretty_string(fragment_dom)
            log.info(pretty_frag_dom)

            ###########################################
            # 3) Write the Fragment to disk as standalone MKV file
            ###########################################
            save_dir = 'ENTER_DIRECTORY_PATH_TO_SAVE_FRAGEMENTS'
            frag_file_name = self.last_good_fragment_tags['AWS_KINESISVIDEO_FRAGMENT_NUMBER'] + '.mkv' # Update as needed
            frag_file_path = os.path.join(save_dir, frag_file_name)
            # Uncomment below to enable this function - will take a significant amount of disk space if left running unchecked:
            #log.info('')
            #log.info(f'####### Saving fragment to local disk at: {frag_file_path}')
            #self.kvs_fragment_processor.save_fragment_as_local_mkv(fragment_bytes, frag_file_path)

            ###########################################
            # 4) Extract Frames from Fragment as ndarrays:
            ###########################################
            # Get a ratio of available frames in the fragment as a list of numpy.ndarray's
            # Here we just log the shape of each image array but ndarray lends itself to many powerful 
            # data science, computer vision and video analytic functions in particular.
            one_in_frames_ratio = 5
            log.info('')
            log.info(f'#######  Reading 1 in {one_in_frames_ratio} Frames from fragment as ndarray:')
            fragment_nmrs.append(str(self.last_good_fragment_tags['AWS_KINESISVIDEO_FRAGMENT_NUMBER']))
            ndarray_frames = self.kvs_fragment_processor.get_frames_as_ndarray(fragment_bytes, one_in_frames_ratio)
            for i in range(len(ndarray_frames)):

                ndarray_frame = ndarray_frames[i]
                H, W, _ = ndarray_frame.shape
                log.info(f'Frame-{i} Shape: {ndarray_frame.shape}')
            
                results = license_plate_detector(ndarray_frame, verbose=False)[0]

                cars_detections = [i for i in results.boxes.data.tolist() if int(i[5]) != 0]
                lc_detections = [i for i in results.boxes.data.tolist() if int(i[5]) == 0]

                lc_detections_ = mot_tracker.update(np.asarray([(x1, y1, x2, y2, score) for x1, y1, x2, y2, score, _ in lc_detections])) if len(lc_detections) > 0 else []

                for detections in lc_detections_:
                    if len(detections) == 5 and not math.isnan(detections[0]) and not math.isnan(detections[1]) and not math.isnan(detections[2]) and not math.isnan(detections[3]):
                        x1, y1, x2, y2, track_id =  detections
                        class_id = 0

                        # print(x1, y1, x2, y2, track_id)

                        x1car, y1car, x2car, y2car = get_car([x1, y1, x2, y2], cars_detections)

                        # print(x1car, y1car, x2car, y2car)

                        license_plate_crop = ndarray_frame[int(y1):int(y2), int(x1):int(x2), :]

                        ret, buffer = cv2.imencode('.jpg', license_plate_crop)
                        bytes_io = BytesIO(buffer)

                        first_seen_track = 1 if track_id not in processed_track_ids else 0
                        if track_id not in processed_track_ids:
                        	processed_track_ids.append(track_id)
                        key = '{}_{}_{}_{}.jpg'.format(str(int(track_id)), self.last_good_fragment_tags['AWS_KINESISVIDEO_FRAGMENT_NUMBER'], str(int(one_in_frames_ratio * i)), first_seen_track)
                        if int(x1car) != -1: s3_client.upload_fileobj(bytes_io, bucket_name, key)

                        new_row = {'fragment_number': str(self.last_good_fragment_tags['AWS_KINESISVIDEO_FRAGMENT_NUMBER']), 
                                    'abs_frame_number': counter + int(one_in_frames_ratio * i),
                                    'frame_number': int(one_in_frames_ratio * i),
                                    'x1': float(x1),
                                    'y1': float(y1),
                                    'x2': float(x2),
                                    'y2': float(y2),
                                    'x1car': float(x1car),
                                    'y1car': float(y1car),
                                    'x2car': float(x2car),
                                    'y2car': float(y2car),
                                    'class_id': int(class_id) if int(class_id) in [0, 1] else 1,
                                    'track_id': int(track_id)
                                    }
                        if int(x1car) != -1: data.loc[len(data.index)] = list(new_row.values())


            unique_values = data[(data['class_id'] == 0) & (data['fragment_number'] == str(self.last_good_fragment_tags['AWS_KINESISVIDEO_FRAGMENT_NUMBER']))]['track_id'].unique()
            print(unique_values)
            for value in unique_values:
                # data_ = pd.concat([data, tmp], ignore_index=True)
                new_df = data[data['track_id'] == value]
                all_values_abs_frames = list(range(min(new_df['abs_frame_number']), 1 + max(new_df['abs_frame_number'])))
                missing_values = list(set(all_values_abs_frames) - set(list(new_df['abs_frame_number'])))
                new_rows = [{'abs_frame_number': value_, 'class_id': new_df['class_id'].iloc[0], 'track_id': new_df['track_id'].iloc[0]} for value_ in missing_values]
                new_df_ = pd.DataFrame(new_rows)
                new_df = pd.concat([new_df, new_df_], ignore_index=True)
                new_df = new_df.sort_values(by='abs_frame_number')
                new_df['x1'] = new_df['x1'].interpolate(method='linear')
                new_df['x2'] = new_df['x2'].interpolate(method='linear')
                new_df['y1'] = new_df['y1'].interpolate(method='linear')
                new_df['y2'] = new_df['y2'].interpolate(method='linear')
                new_df['x1car'] = new_df['x1car'].interpolate(method='linear')
                new_df['x2car'] = new_df['x2car'].interpolate(method='linear')
                new_df['y1car'] = new_df['y1car'].interpolate(method='linear')
                new_df['y2car'] = new_df['y2car'].interpolate(method='linear')
                new_df['frame_number'] = new_df['abs_frame_number'] % 250
                new_df['fragment_number'].fillna(method='ffill', inplace=True)
                data = pd.concat([data, new_df])
                data = data.sort_values(by='abs_frame_number')



            data = data.drop_duplicates(keep='first')

            # data.to_csv('tmp.csv', index=False)
            # s3_client.upload_file('tmp.csv', bucket_name, 'tmp.csv')

            if len(fragment_nmrs) > 0:
                df = data[data['fragment_number']==fragment_nmrs[-1]]

                # df.to_csv('tmp.csv', index=False)
                # s3_client.upload_file('tmp.csv', bucket_name, '{}.csv'.format(fragment_nmrs[-1]))

                table = dynamodb_client.Table(table_name)

                with table.batch_writer() as batch:
                    for index, row in df.iterrows():
                        item = convert_to_dynamodb_item(row)
                        print(item)
                        batch.put_item(Item=item)

                response = sqs_client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=json.dumps({'fragment_number': fragment_nmrs[-1]}),
                    MessageGroupId=fragment_nmrs[-1],
                    MessageDeduplicationId=random_string()
                )
            ###########################################
            # 5) Save Frames from Fragment to local disk as JPGs
            ###########################################
            # Get a ratio of available frames in the fragment and save as JPGs to local disk.
            # JPEGs could also be sent to other AWS services such as Amazon Rekognition and Amazon Sagemaker
            # for computer vision inference. 
            # Alternatively, these could be sent to Amazon S3 and used to create a timelapse set of images or 
            # further processed into timed thumbnails for the KVS media stream.
            one_in_frames_ratio = 5
            save_dir = 'ENTER_DIRECTORY_PATH_TO_SAVE_JPEG_FRAMES'
            jpg_file_base_name = self.last_good_fragment_tags['AWS_KINESISVIDEO_FRAGMENT_NUMBER']
            jpg_file_base_path = os.path.join(save_dir, jpg_file_base_name)
            
            # Uncomment below to enable this function - will take a significant amount of disk space if left running unchecked:
            #log.info('')
            #log.info(f'####### Saving 1 in {one_in_frames_ratio} Frames from fragment as JPEG to base path: {jpg_file_base_path}')
            #jpeg_paths = self.kvs_fragment_processor.save_frames_as_jpeg(fragment_bytes, one_in_frames_ratio, jpg_file_base_path)
            #for i in range(len(jpeg_paths)):
            #    jpeg_path = jpeg_paths[i]
            #    print(f'Saved JPEG-{i} Path: {jpeg_path}')

            counter += 250

        except Exception as err:
            log.error(f'on_fragment_arrived Error: {err}')
    
    def on_stream_read_complete(self, stream_name):
        '''
        This callback is triggered by the KvsConsumerLibrary when a stream has no more fragments available.
        This represents a graceful exit of the KvsConsumerLibrary thread.

        A stream will reach the end of the available fragments if the StreamSelector applied some 
        time or fragment bounding on the media request or if requesting a live steam and the producer 
        stopped sending more fragments. 

        Here you can choose to either restart reading the stream at a new time or just clean up any
        resources that were expecting to process any further fragments. 
        
        ### Parameters:

            **stream_name**: str
                Name of the stream as set when the KvsConsumerLibrary thread triggering this callback was initiated.
                Use this to identify a fragment when multiple streams are read from different instances of KvsConsumerLibrary to this callback.
        '''
        print(f'Read Media on stream: {stream_name} Completed successfully - Last Fragment Tags: {self.last_good_fragment_tags}')

    def on_stream_read_exception(self, stream_name, error):
        '''
        This callback is triggered by an exception in the KvsConsumerLibrary reading a stream. 
        
        For example, to process use the last good fragment number from self.last_good_fragment_tags to
        restart the stream from that point in time with the example stream selector provided below. 
        
        Alternatively, just handle the failed stream as per your application logic requirements.

        ### Parameters:

            **stream_name**: str
                Name of the stream as set when the KvsConsumerLibrary thread triggering this callback was initiated.
                Use this to identify a fragment when multiple streams are read from different instances of KvsConsumerLibrary to this callback.

            **error**: err / exception
                The Exception obje tvthat was thrown to trigger this callback.

        '''

        # Can choose to restart the KvsConsumerLibrary thread at the last received fragment with below example StartSelector
        #StartSelector={
        #    'StartSelectorType': 'FRAGMENT_NUMBER',
        #    'AfterFragmentNumber': self.last_good_fragment_tags['AWS_KINESISVIDEO_CONTINUATION_TOKEN'],
        #}

        # Here we just log the error 
        print(f'####### ERROR: Exception on read stream: {stream_name}\n####### Fragment Tags:\n{self.last_good_fragment_tags}\nError Message:{error}')

    ####################################################
    # KVS Helpers
    def _get_data_endpoint(self, stream_name, api_name):
        '''
        Convenience method to get the KVS client endpoint for specific API calls. 
        '''
        response = self.kvs_client.get_data_endpoint(
            StreamName=stream_name,
            APIName=api_name
        )
        return response['DataEndpoint']

if __name__ == "__main__":
    '''
    Main method for example KvsConsumerLibrary
    '''
    
    kvsConsumerExample = KvsPythonConsumerExample()
    kvsConsumerExample.service_loop()
