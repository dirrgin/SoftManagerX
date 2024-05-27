import sys
from manager import \
    IoTHubRegistryManager, \
    receive_twin_reported, \
    twin_desired, json,\
    clear_desired_twin,\
     asyncio, os,load_dotenv,\
    BlobServiceClient, monitor_blob_container

load_dotenv()
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
CONNECTION_STRING_MANAGER = os.getenv("CONNECTION_STRING_MANAGER")
DEVICE_ID = os.getenv("DEVICE_ID")
BLOB_CONTAINER_NAME = "production-rate"
async def main():
 
    registry_manager = IoTHubRegistryManager(CONNECTION_STRING_MANAGER)
    await clear_desired_twin(registry_manager, DEVICE_ID)
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    
    try:
        while True:        
            twin_reported = await receive_twin_reported(registry_manager, DEVICE_ID)
            
            await twin_desired(registry_manager, DEVICE_ID, twin_reported)
            await monitor_blob_container(blob_service_client, BLOB_CONTAINER_NAME, registry_manager)

    except Exception as e:
        print("Progam stoped")
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())