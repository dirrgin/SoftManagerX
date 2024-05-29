import sys
from datetime import datetime
from manager import \
    IoTHubRegistryManager, \
    receive_twin_reported, \
    twin_desired, json,\
    clear_desired_twin,Twin, TwinProperties,\
    asyncio, os,load_dotenv,\
    BlobServiceClient, get_most_recent_blob, process_blob

load_dotenv()
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
CONNECTION_STRING_MANAGER = os.getenv("CONNECTION_STRING_MANAGER")
DEVICE_ID = os.getenv("DEVICE_ID")
BLOB_CONTAINER_NAME = "production-rate"
async def main():
 
    registry_manager = IoTHubRegistryManager(CONNECTION_STRING_MANAGER)
    await clear_desired_twin(registry_manager, DEVICE_ID)
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    last_processed_name = None
    last_processed_time = datetime.min  # Initialize to the earliest possible datetime
    try:
        while True:        
            twin_reported = await receive_twin_reported(registry_manager, DEVICE_ID)
            kpi_container_name = BLOB_CONTAINER_NAME
            container_client = blob_service_client.get_container_client(kpi_container_name)
            try:
            # List blobs synchronously
                most_recent_blob = await get_most_recent_blob(container_client, last_processed_name, last_processed_time)
                if most_recent_blob:
                    last_processed_name, last_processed_time = await process_blob(most_recent_blob, registry_manager, twin_reported, container_client)
                else:
                    print("no fresh blobs. last last_processed_name is ", last_processed_name)
            except Exception as e:
                print(f"Exception: {str(e)}")
            await asyncio.sleep(60)
                        

    except Exception as e:
        print("Progam stoped")
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())