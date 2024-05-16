import asyncio
from azure.iot.hub import IoTHubRegistryManager
from azure.iot.hub.models import Twin, TwinProperties, CloudToDeviceMethod

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