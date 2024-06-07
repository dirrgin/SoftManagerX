import sys, aioconsole
from manager import \
    IoTHubRegistryManager, \
    receive_twin_reported, \
    twin_desired, json,\
    clear_desired_twin,Twin, TwinProperties,\
    asyncio, os,load_dotenv,get_most_recent_blob,\
    BlobServiceClient, run_res_error, process_error_dm, datetime

load_dotenv()
BLOB_CONNECTION_STRING = os.getenv("BLOB_CONNECTION_STRING")
CONNECTION_STRING_MANAGER = os.getenv("CONNECTION_STRING_MANAGER")
DEVICE_ID = os.getenv("DEVICE_ID")
KPI_CONTAINER_NAME = "production-rate"
ERROR_CONTAINER_NAME = "device-errors-output"
async def main():
 
    registry_manager = IoTHubRegistryManager(CONNECTION_STRING_MANAGER)
    await clear_desired_twin(registry_manager, DEVICE_ID)
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
    kpi_last_processed_name  = None
    error_last_processed_name = None
    kpi_last_processed_time = datetime.min 
    error_last_processed_time = datetime.min 
    err_last_positions = {}
    try:
        while True:        
            twin_reported = await receive_twin_reported(registry_manager, DEVICE_ID)
            kpi_container_name = KPI_CONTAINER_NAME
            kpi_container_client = blob_service_client.get_container_client(kpi_container_name)
            error_container_name = ERROR_CONTAINER_NAME
            error_container_client = blob_service_client.get_container_client(error_container_name)
            # try:
            #     recent_kpi_blob = await get_most_recent_blob(kpi_container_client, kpi_last_processed_name, kpi_last_processed_time)
            #     if recent_kpi_blob:
            #         kpi_last_processed_name, kpi_last_processed_time = await process_production(recent_kpi_blob, registry_manager, twin_reported, kpi_container_client)
            #     else:
            #         print("no fresh blobs. last kpi_last_processed_name is ", kpi_last_processed_name)
            # except Exception as e:
            #     print(f"Exception: {str(e)}")
            try:
                new_data, error_last_processed_name, error_last_processed_time, err_last_positions = await get_most_recent_blob(error_container_client, error_last_processed_name, error_last_processed_time, err_last_positions)
                if new_data:
                    error_devices = await process_error_dm(registry_manager, new_data)
                    print(f"New data from blob {error_last_processed_name} arrived")
                    answer = "empty"
                    while answer.lower() != "no":
                        answer = await aioconsole.ainput(f"Reset error status for device(s) {error_devices}? (No/device name): ")
                        if answer.lower() != "no":
                            await run_res_error(registry_manager, answer)
                else:
                    print("No new data found")
                
                 
                

            except Exception as ee:
                print(f"Exception with Error Container process: {ee}")

            await asyncio.sleep(60)
                        

    except Exception as e:
        print("Progam stoped")
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())