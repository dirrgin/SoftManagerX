import asyncio, json, os, aiohttp
from dotenv import load_dotenv
from azure.iot.hub import IoTHubRegistryManager
from azure.iot.hub.models import Twin, TwinProperties, CloudToDeviceMethod
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage
load_dotenv()
hostname = os.getenv('IOTHUB_HOSTNAME')
servicebus_connection_str = os.getenv('AZURE_SERVICE_BUS_CONNECTION_STRING')
queue_name1 = os.getenv('QUEUE_CREATE')
queue_name2 = os.getenv('QUEUE_CONNECTIONS')

async def create_device(device_id,registry_manager):
    try:
        device_connection_string = ""
        try:
            device = registry_manager.get_device(device_id)
            print(f"Production Line {device_id} already exists.")
        except Exception:
            device = registry_manager.create_device_with_sas(
                device_id,
                primary_key=None,
                secondary_key=None,
                status="enabled"
            )
            primary_key = device.authentication.symmetric_key.primary_key
            device_connection_string = f"HostName={hostname};DeviceId={device_id};SharedAccessKey={primary_key}"
            print(f"Device {device_id} created successfully.")
            return device, device_connection_string
    except Exception as e:
        print(f"Error creating device {device_id}: {e}")
        return None, None

async def create_devices(device_ids,registry_manager):
    tasks = [create_device(device_id, registry_manager) for device_id in device_ids]
    # Gather results
    results = await asyncio.gather(*tasks)
    created_devices, device_connection_strings = zip(*results)  
    return created_devices, device_connection_strings

async def send_device_connection_keys(device_connection_strs):
    async with ServiceBusClient.from_connection_string(servicebus_connection_str) as client:
            sender = client.get_queue_sender(queue_name2)
            async with sender:
                print("!@")
                for conn_strs in device_connection_strs:
                    message = ServiceBusMessage(conn_strs)  
                    await sender.send_messages(message)
                    print(f"Sent device CONNECTION STRING")

async def receive_device_ids(registry_manager):
    created_devices = []
    created_connection_strs = []
    async with ServiceBusClient.from_connection_string(servicebus_connection_str) as client:
        receiver = client.get_queue_receiver(queue_name1)
        async with receiver:
            while True:
                try:
                    received_msgs = await receiver.receive_messages(max_message_count=100, max_wait_time=1)
                    if not received_msgs:
                        print("Likely no more messages there.")
                        break
                    for msg in received_msgs:
                        device_id = str(msg)
                        devices, connection_strs = await create_devices([device_id], registry_manager)
                        print(f"Created devices: {devices}")
                        print(f"Created connection strings: {connection_strs}")

                        created_devices.extend(devices)
                        created_connection_strs.extend(connection_strs)

                        await receiver.complete_message(msg)
                except asyncio.TimeoutError:
                    print("Timeout reached while waiting for messages.")
                    break
    print(f"Processed all IDs: {created_connection_strs}")
    return created_devices, created_connection_strs

async def receive_twin_reported(manager_client, device_id):
    twin = manager_client.get_twin(device_id)
    rep = twin.properties.reported
    return rep

'''async def handle_c2d_message(message):
    print("Received message: ", message)
    payload = message.data
    message_json = json.loads(payload)'''

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

async def run_res_error(registry_manager, device_id):
    reset_method = CloudToDeviceMethod(method_name="reset_err_status", payload=None, response_timeout_in_seconds=14)
    registry_manager.invoke_device_method(device_id, reset_method)

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
        # Sort blobs by most recent first
        sorted_blobs = sorted(blobs, key=lambda blob: blob['last_modified'], reverse=True)
        # Get the current time current_time = datetime.utcnow()
        for blob in sorted_blobs:
            # if (blob.name != last_processed_name 
            #     and blob['last_modified'].replace(tzinfo=None) > last_processed_time 
            #     and current_time - blob['last_modified'].replace(tzinfo=None) <= timedelta(hours=1)):
            if (blob.name != last_processed_name 
                and blob['last_modified'].replace(tzinfo=None) > last_processed_time):  
                #print(blob.name)  
                last_position = last_read_positions.get(blob.name, 0)
                new_data, last_position = await read_new_lines_from_blob(container_client, blob.name, last_position)

                last_read_positions[blob.name] = last_position
                return new_data, blob.name, blob['last_modified'], last_read_positions

    except Exception as e:
        print(f"recent_blob exception: {e}")

    return None, last_processed_name, last_processed_time, last_read_positions

async def process_production(registry_manager, new_data, productionLine):
    unique_data = {}
    for line in new_data:
        try:
            device_name = line['DeviceName']
            if device_name in productionLine and line['ProductionPercent'] != 'NaN':
                if (line['ProductionRate'] - 10) < 0:
                    unique_data[device_name] = 0
                else:
                    unique_data[device_name] = line['ProductionRate'] - 10

        except json.JSONDecodeError as json_err:
            print(f"JSON decoding error: {str(json_err)} in line: {line}")
        except KeyError as key_err:
            print(f"Key error: {str(key_err)} in line: {key_err}")
    try:
        for device_name, rate in unique_data.items():
            try:
                twin_reported = {"ProductionRate": rate}
                twin = registry_manager.get_twin(device_name)
                twin_patch = Twin(properties=TwinProperties(desired=twin_reported))
                twin = registry_manager.update_twin(device_name, twin_patch, twin.etag)
            except Exception as patch:
                print(f"twin patch problem: {str(patch)} in line: {line}")
    except KeyError as key_err:
        print(f"Key error 2: {str(key_err)} in line: {line}")

#Emergency Stop Direct Method
async def process_error_dm(registry_manager, new_data, productionLine):
    try:   
        unique_data = []
        for line in new_data:
            try:
                device_name = line['DeviceName']
                if device_name in productionLine and device_name not in unique_data:
                    unique_data.append(device_name)
            except json.JSONDecodeError as json_err:
                print(f"JSON decoding error: {str(json_err)} in line: {line}")
            except KeyError as key_err:
                print(f"Key error: {str(key_err)} in line: {line}")
        for device_name in unique_data:
            try:
                stop_method = CloudToDeviceMethod(method_name="emergency_stop", payload=None, response_timeout_in_seconds=14)
                registry_manager.invoke_device_method(device_name, stop_method)
                print(f"Success invoking method for device {device_name}")
            except Exception as method_err:
                print(f"Error invoking method for device {device_name}: {str(method_err)}")
        return unique_data
    except aiohttp.ClientResponseError as e:
        # Handle specific HTTP errors
        print(f"HTTP error: {e.status}, {e.message}")
    except Exception as e:
        print(f"Exception in process_error_dm: {str(e)}")
