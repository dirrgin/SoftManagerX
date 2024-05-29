import asyncio, logging, json, os
from dotenv import load_dotenv
from azure.iot.hub import IoTHubRegistryManager
from azure.iot.hub.models import Twin, TwinProperties, CloudToDeviceMethod
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
load_dotenv()
DEVICE_ID = os.getenv("DEVICE_ID")
async def receive_twin_reported(manager_client, device_id):
    twin = manager_client.get_twin(device_id)
    rep = twin.properties.reported
    print("\tTwin reported:")
    print(rep)
    return rep

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

async def read_blob_content(blob_client):
    downloader =  blob_client.download_blob()
    return downloader.readall().decode('utf-8')

async def get_most_recent_blob(container_client, last_processed_name, last_processed_time):
    blobs = container_client.list_blobs()
    # Sort blobs by last_modified attribute, most recent first
    sorted_blobs = sorted(blobs, key=lambda blob: blob['last_modified'], reverse=True)
    for blob in sorted_blobs:
        if (blob.name != last_processed_name) and (blob['last_modified'].replace(tzinfo=None) > last_processed_time):
            return blob
    return None


async def process_blob(blob, registry_manager, twin_reported, container_client):
    blob_client = container_client.get_blob_client(blob.name)
    new_data = await read_blob_content(blob_client)

    # Process new data
    # new_data = blob_content 
    twin_reported.pop("$metadata", None)
    twin_reported.pop("$version", None)
    json_lines = new_data.splitlines()

    # Process each JSON line
    for line in json_lines:
        data = json.loads(line)  # Parse the JSON line into a dictionary
        prod_rate = data['ProductionRate'] - 10
        print(data)
        print("its new PR: ", prod_rate)
        formatted_key = data['DeviceName'].replace(" ", "")  # Remove spaces from the key
        if prod_rate < 0:
            prod_rate = 0
        twin_reported[formatted_key] = {"ProductionRate": prod_rate}
        twin = registry_manager.get_twin(DEVICE_ID)
        twin_patch = Twin(properties=TwinProperties(desired=twin_reported))
        twin = registry_manager.update_twin(DEVICE_ID, twin_patch, twin.etag)
    return blob.name, blob['last_modified'].replace(tzinfo=None)
        
        