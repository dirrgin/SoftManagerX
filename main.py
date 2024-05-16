import sys
import time
from manager import \
    IoTHubRegistryManager, \
    receive_twin_reported, \
    twin_desired, \
    clear_desired_twin,\
    asyncio


async def main():
    CONNECTION_STRING_MANAGER = "HostName=Cirencester-End.azure-devices.net;SharedAccessKeyName=iothubowner;SharedAccessKey=4THcWqn7NQdn21nYhOcsUm6iWCa3Z4knLAIoTDcDVKI="
    DEVICE_ID = "demo_device1"

    registry_manager = IoTHubRegistryManager(CONNECTION_STRING_MANAGER)
    await clear_desired_twin(registry_manager, DEVICE_ID)
    inKey=1
    try:
        while True:
            # valid_choice = False
            # while not valid_choice:
            #     try:
            #         print("""
            #             Please select an option:
            #             1 - no opt
            #             2 - Direct Method
            #             3 - Set desired options
            #             0 - Exit
            #             """)
            #         inKey = int(input('Enter your choice: '))
            #         if inKey in [0, 1, 2, 3]:
            #             if inKey == 0 :
            #                 print("Progam stoped")
            #                 break;
            #             elif inKey == 3:
                            
            #         else:
            #             print("Please enter a number from the menu.")
            #     except ValueError:
            #         print("Please enter a valid number.")
            
            twin_reported = await receive_twin_reported(registry_manager, DEVICE_ID)
            print("twin_reported works : ")
            # sending the twin desired
            await twin_desired(registry_manager, DEVICE_ID, twin_reported)
            

    except Exception as e:
        print("Progam stoped")
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())