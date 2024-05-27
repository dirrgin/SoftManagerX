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

    reported.pop("$metadata", None)
    reported.pop("$version", None)
    
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

# Function to read the content of a blob asynchronously
async def read_blob_content(blob_client):
    stream = blob_client.download_blob()
    blob_data = stream.readall()
    return blob_data.decode('utf-8')

async def monitor_blob_container(blob_service_client, container_name, iot_registry_manager):
    container_client = blob_service_client.get_container_client(container_name)
    
    # Keep track of the last processed blob and its length
    last_blob_name = None
    last_blob_length = 0
    
    while True:
        # List blobs synchronously
        blobs = container_client.list_blobs()
        
        for blob in blobs:
            blob_client = container_client.get_blob_client(blob.name)
            #blob_properties = await blob_client.get_blob_properties()

            # Read the full content of the blob
            blob_content = await read_blob_content(blob_client)
            
            # If it's the first time processing this blob, or if it has grown, process new data
            if blob.name != last_blob_name or len(blob_content) > last_blob_length:
                new_data = blob_content[last_blob_length:]  # Only process new content
                if container_name == "production-rate":
                    json_lines = new_data.splitlines()
                    updated_devices = {}
                    # Process each JSON line
                    for line in json_lines:
                        data = json.loads(line)  # Parse the JSON line into a dictionary
                        prod_rate = data['ProductionRate'] - 10
                        if prod_rate < 0:
                            prod_rate = 0
                        updated_devices[data['DeviceName']] = {"ProductionRate": prod_rate}
                    await twin_desired(iot_registry_manager, DEVICE_ID, updated_devices)
               
                last_blob_name = blob.name
                last_blob_length = len(blob_content)
        
        