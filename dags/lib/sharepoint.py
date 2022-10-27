from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.folders.folder import Folder


def connect_with_user_credential():
    site_url = "https://kpimvn.sharepoint.com"
    ctx = ClientContext(site_url).with_credentials(
        UserCredential("hai.nguyen@kpim.vn", "xxxxxx"))
    web = ctx.web
    ctx.load(web)
    ctx.execute_query()
    print("Web title: {0}".format(web.properties['Title']))


def test_upload_file(site_url: str):
    sharepoint_url = "https://kpimvn.sharepoint.com/sites/dataengineer"
    ctx = ClientContext(sharepoint_url).with_credentials(
        UserCredential("hai.nguyen@kpim.vn", "xxxxxx"))
    
    # relative_url = "/sites/dataengineer/Shared Documents"
    relative_url = "/sites/dataengineer/Shared Documents/Data Warehouse SQL Server/Industry - Stock/CS01/2. Data Warehouse/1. Data Source Layer"
    
    target_folder = ctx.web.get_folder_by_server_relative_url(relative_url)
    target_folder.expand(["Folders"]).get().execute_query()

    for folder in target_folder.folders:
        print(folder.serverRelativeUrl)
        print(folder.time_created)

    # target_folder = ctx.web.get_folder_by_server_relative_url(relative_url)
    # print(target_folder.name)
    # print(target_folder.serverRelativeUrl)
    # print(target_folder.get_metadata)

    # https://kpimvn.sharepoint.com/sites/dataengineer/Shared Documents","
