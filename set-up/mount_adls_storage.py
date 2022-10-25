# Databricks notebook source
# MAGIC %md
# MAGIC #### Initialization

# COMMAND ----------

###############################################################################
# Create an Azure Service Principal and give it access to our storage account #
###############################################################################
# vedio 25,26,27
# step 1: Create Service Principal account
# - Goto Azure Active Directory > App Registration > New Registration: databricks-course-service-app
# > Copy the 'Application (client) ID' & 'Directory (tenant) ID' into a NOTEPAD
# - Click on Certificates & secrets > New client secret > Description: databricks-course-access-secret
# > Copy the secret value into a NOTEPAD
# step 2: Give storage account access to the Service Principal account
# - Goto Storage accounts > Select it: adfcourseanyistaccdl > Access Control (IAM) 
#   > Add role assignment > Storage Blob Data Contributor > User, group, or service principal > Select members 
#   > search for the Service Principal account you just created:databricks-course-service-app > select > create


####Crdentials are exposed hence we should use Azure Key vaults to keep our secrets (secrets/createScope)
### Vedio 29

storage_account_name = "adfcourseanyistaccdl"
client_id = "62fb37db-0bdd-4b2c-9e4f-a70b21ff40f2"
tenant_id = "865c4c90-6099-4d1b-af3d-46fb27d6958e"
client_secret = "fX58Q~hubxzngYj51~RAP2jm3bS.rdiwRk5ntb8O"

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setup Configurations

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": f"{client_id}",
    "fs.azure.account.oauth2.client.secret": f"{client_secret}",
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mount our storage

# COMMAND ----------

# mount the container
#container_name = "raw-ctnr"
#dbutils.fs.mount(
#    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
#    mount_point = f"/mnt/{storage_account_name}/{container_name}",
#    extra_configs = configs
#)

# COMMAND ----------

# function to mount the containers
def mount_adls(container_name):
    # mount the container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs
    )

# COMMAND ----------

raw_ctnr_name = "raw-ctnr"
proc_ctnr_name = "processed-ctnr"

mount_adls(raw_ctnr_name)
mount_adls(proc_ctnr_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #### List all files in our mount

# COMMAND ----------

# list the files in our mount
# /mnt/adfcourseanyistaccdl/raw-ctnr
dbutils.fs.ls("/mnt/{}/{}".format(storage_account_name, raw_ctnr_name))
dbutils.fs.ls("/mnt/{}/{}".format(storage_account_name, proc_ctnr_name))

# COMMAND ----------

# MAGIC %md
# MAGIC #### List all the mounts in our workspace

# COMMAND ----------

# list all the mounts in our workspace
display(dbutils.fs.mounts())

# COMMAND ----------


