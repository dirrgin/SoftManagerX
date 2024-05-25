import asyncio
from azure.iot.hub import IoTHubRegistryManager
from azure.iot.hub.models import Twin, TwinProperties, CloudToDeviceMethod
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
async def receive_twin_reported(manager_client, device_id):
    twin = manager_client.get_twin(device_id)
    rep = twin.properties.reported
    print("Twin reported:")
    print(rep)
    return rep

async def twin_desired(manager_client, device_id, reported):
    desired_twin = {}

    del reported["$metadata"]
    del reported["$version"]

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
    stream = await blob_client.download_blob()
    blob_data = await stream.readall()
    return blob_data.decode('utf-8')

# Function to process newly appended data
async def process_data(new_data, iot_registry_manager):
    json_lines = new_data.splitlines()
    for line in json_lines:
        data = json.loads(line)
        device_name = data['DeviceName']
        dev_num = device_name.split()[-1]  # assuming 'Device X' format
        method_name = "emergency_stop"
        payload = {"DeviceName": dev_num}

        # Create the method payload
        method = CloudToDeviceMethod(
            method_name=method_name,
            payload=json.dumps(payload),
            connect_timeout_in_seconds=10,
            response_timeout_in_seconds=10
        )

        # Invoke direct method on the device
        response = await iot_registry_manager.invoke_device_method(device_name, method)
        logging.info(f"Direct method response: {response}")


async def monitor_blob_container(blob_service_client, container_name):
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
                await process_data(new_data)
                
                # Update the last processed blob and its length
                last_blob_name = blob.name
                last_blob_length = len(blob_content)
        
        # Wait before checking again (adjust the interval as needed)
        await asyncio.sleep(60)