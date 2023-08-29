import azure.functions as func
from azure.mgmt.sql import SqlManagementClient
from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
import json
import os

import requests
import logging
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.function_name(name="HttpTrigger")
@app.route(route="", auth_level=func.AuthLevel.ANONYMOUS)
def HttpTrigger(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('Python HTTP trigger function processed a request.')
        request_json = req.get_json()
        logging.info(request_json)
        deployment_name = request_json['recoveryName']
        primary_resource_metadata_url = request_json['resourceMapping']['primaryResourceMetadataPath']
        recovered_metadata_url = request_json['resourceMapping']['recoveredMetadataPath']
        rec_id = request_json['recoveryId']
        # source_recovery_mapping_url = request_json['resourceMapping']['sourceRecoveryMappingPath']

        # Send GET requests and print the JSON responses
        json1 = requests.get(recovered_metadata_url).json()
        logging.info(json1)

        for item in json1:
            for key, value in item.items():
                for item_data in value:
                    recovery_resource_group = item_data['groupIdentifier']
                    recovery_region = item_data['region'].replace(
                        ' ', '').lower()
                    subscription_id = item_data['cloudResourceReferenceId'].split(
                        "/")[2]
                    break

        # Send GET requests and print the JSON responses
        json2 = requests.get(primary_resource_metadata_url).json()
        logging.info(json1)

        for item in json2:
            for key, value in item.items():
                for item_data in value:
                    resource_group_name = item_data['groupIdentifier']
                    primary_location = item_data['region'].replace(' ', '').lower()
                    # recovery_subscription_id = item_data['cloudResourceReferenceId'].split("/")[2]
                    break
        if "resetUser" in request_json:
            primary_location,recovery_region=recovery_region,primary_location


        client_id = os.environ["CLIENT_ID"]
        client_secret = os.environ["CLIENT_SECRET"]
        tenant_id = os.environ["TENANT_ID"]

        # Create a client secret credential object
        credentials = ClientSecretCredential(
            client_id=client_id,
            client_secret=client_secret,
            tenant_id=tenant_id
        )

        client = SqlManagementClient(credentials, subscription_id)
        resource_client = ResourceManagementClient(credentials, subscription_id)
        sql_client = SqlManagementClient(credentials, subscription_id)
        server_list = []

        for item in resource_client.resources.list_by_resource_group(resource_group_name):
            if item.type == "Microsoft.Sql/servers" and item.location == primary_location:
                server_list.append(item)

        for server in server_list:
            server_name = server.name
            failover_groups = sql_client.failover_groups.list_by_server(resource_group_name, server_name)
            for failover_group in failover_groups:
                print(failover_group)
                for part_server in failover_group.partner_servers:
                    print(part_server)
                    if part_server.location.lower().replace(" ", "") == recovery_region:
                        print(f"Promoting failover for server {server_name}...")
                        temp = part_server.id.split("/")
                        part_server_name = temp[-1]
                        
                        sql_client.failover_groups.begin_force_failover_allow_data_loss(resource_group_name, part_server_name , failover_group.name)
                        print(f"Failover promoted for server {server_name} to {part_server_name}")
                    else:
                        print(f"Secondary server for failover group {failover_group.name} is not in the West region.")
        logging.info("End Of Function")

        return func.HttpResponse(
            "200",
            status_code=200)

    except Exception as e:
        logging.error(f"Error occurred: {str(e)}")
        return func.HttpResponse(f"Hello, {str(e)}. This HTTP triggered function executed successfully.", status_code=400)
