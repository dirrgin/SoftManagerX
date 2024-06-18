import asyncio, logging, json, os, aiohttp
from datetime import datetime, timedelta
from dotenv import load_dotenv
from azure.iot.hub import IoTHubRegistryManager
from azure.iot.hub.models import Twin, TwinProperties, CloudToDeviceMethod
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
load_dotenv()
DEVICE_ID = os.getenv("DEVICE_ID")
async def receive_twin_reported(manager_client, device_id):
    twin = manager_client.get_twin(device_id)
    rep = twin.properties.reported
    return rep

async def handle_c2d_message(message):
    print("Received message: ", message)
    payload = message.data
    message_json = json.loads(payload)
    print(message_json)

async def twin_desired(manager_client, device_id, reported):
    desired_twin = {}
    for key, value in reported.items():
        desired_twin[key] = {"ProductionRate": value["ProductionRate"]}
    twin = manager_client.get_twin(device_id)
    twin_patch = Twin(properties=TwinProperties(desired=desired_twin))
    twin = manager_client.update_twin(device_id, twin_patch, twin.etag)

async def clear_desired_twin(manager, device_id):
    twin = manager.get_twin(device_id)
    des = twin.properties.desired
    del des["$metadata"]
    del des["$version"]
    for key, value in des.items():
        des[key] = None
    twin_patch = Twin(properties=TwinProperties(desired=des))
    twin = manager.update_twin(device_id, twin_patch, twin.etag)
    print("desired prop were just cleaned")

async def run_res_error(registry_manager, device_name):
    reset_method = CloudToDeviceMethod(method_name="reset_err_status", payload=device_name, response_timeout_in_seconds=14)
    registry_manager.invoke_device_method(DEVICE_ID, reset_method)

async def read_new_lines_from_blob(container_client, blob_name, last_position):
    try:
        blob_client = container_client.get_blob_client(blob_name)
        # Read the blob from the last read position to the end
        blob_properties =  blob_client.get_blob_properties()
        new_length = blob_properties.size
        new_lines_data = []
        if new_length > last_position:
            blob_data =  blob_client.download_blob(offset=last_position, length=new_length-last_position)
            new_lines =  blob_data.readall()
            last_position = new_length
            new_lines_str = new_lines.decode('utf-8').strip().split('\n')
            for line in new_lines_str:
                json_data = json.loads(line)
                new_lines_data.append(json_data)
        return new_lines_data, last_position
    except Exception as e:
        print(f"An error occurred while reading new lines from blob: {e}")
        return [], last_position

async def get_most_recent_blob(container_client, last_processed_name, last_processed_time, last_read_positions):
    try:
        blobs = container_client.list_blobs()
        # Sort blobs by last_modified attribute, most recent first
        sorted_blobs = sorted(blobs, key=lambda blob: blob['last_modified'], reverse=True)
        
        # Get the current time current_time = datetime.utcnow()

        for blob in sorted_blobs:
            # if (blob.name != last_processed_name 
            #     and blob['last_modified'].replace(tzinfo=None) > last_processed_time 
            #     and current_time - blob['last_modified'].replace(tzinfo=None) <= timedelta(hours=1)):
            if (blob.name != last_processed_name 
                and blob['last_modified'].replace(tzinfo=None) > last_processed_time):  
                print(blob.name)  
                last_position = last_read_positions.get(blob.name, 0)
                new_data, last_position = await read_new_lines_from_blob(container_client, blob.name, last_position)

                # Update last read positions
                last_read_positions[blob.name] = last_position
                return new_data, blob.name, blob['last_modified'], last_read_positions

    except Exception as e:
        print(f"recent_blob exception: {e}")

    return None, last_processed_name, last_processed_time, last_read_positions

async def process_production(registry_manager, twin_reported, new_data):
    twin_reported.pop("$metadata", None)
    twin_reported.pop("$version", None)
    # Process each JSON line
    try:
        unique_data = {}
        for line in new_data:
            try:
                device_name = line['DeviceName']
                if line['ProductionPercent'] != 'NaN':
                    unique_data[device_name] = line['ProductionRate'] 
            except json.JSONDecodeError as json_err:
                print(f"JSON decoding error: {str(json_err)} in line: {line}")
            except KeyError as key_err:
                print(f"Key error: {str(key_err)} in line: {key_err}")

            for device_name, rate in unique_data.items():
                print(f"{device_name} has {rate}-10")
                prod_rate = rate - 10
                formatted_key = device_name.replace(" ", "")
                if prod_rate < 0:
                    prod_rate = 0
                unique_data[device_name] = prod_rate
                try:
                    twin_reported[formatted_key] = {"ProductionRate": prod_rate}
                    twin = registry_manager.get_twin(DEVICE_ID)
                    twin_patch = Twin(properties=TwinProperties(desired=twin_reported))
                    twin = registry_manager.update_twin(DEVICE_ID, twin_patch, twin.etag)
                except Exception as patch:
                    print(f"twin patch problem: {str(patch)} in line: {line}")
    except KeyError as key_err:
        print(f"Key error 2: {str(key_err)} in line: {line}")

#Emergency Stop Direct Method
async def process_error_dm(registry_manager, new_data):
    try:   
        unique_data = []
        for line in new_data:
            try:
                device_name = line['DeviceName']
                if device_name not in unique_data:
                    unique_data.append(device_name)
            except json.JSONDecodeError as json_err:
                print(f"JSON decoding error: {str(json_err)} in line: {line}")
            except KeyError as key_err:
                print(f"Key error: {str(key_err)} in line: {line}")
        for device_name in unique_data:
            try:
                payload = device_name
                stop_method = CloudToDeviceMethod(method_name="emergency_stop", payload=payload, response_timeout_in_seconds=14)
                registry_manager.invoke_device_method(DEVICE_ID, stop_method)
                print(f"Success invoking method for device {device_name}")
            except Exception as method_err:
                print(f"Error invoking method for device {device_name}: {str(method_err)}")
        return unique_data
    except aiohttp.ClientResponseError as e:
        # Handle specific HTTP errors
        print(f"HTTP error: {e.status}, {e.message}")
    except Exception as e:
        print(f"Exception in process_error_dm: {str(e)}")
