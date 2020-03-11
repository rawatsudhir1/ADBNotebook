// Databricks notebook source
val storageAccountName = "sudhirawmdwstorage"
val appID = "7ae659f5-740e-47d5-bc17-1f42ae574841"
val secret = "Z7:[=@g1qhsF7EyrAGIWa5g=E:@2ftcs"
val fileSystemName = "mystorage"
val tenantID = "72f988bf-86f1-41af-91ab-2d7cd011db47"

spark.conf.set("fs.azure.account.auth.type." + storageAccountName + ".dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type." + storageAccountName + ".dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id." + storageAccountName + ".dfs.core.windows.net", "" + appID + "")
spark.conf.set("fs.azure.account.oauth2.client.secret." + storageAccountName + ".dfs.core.windows.net", "" + secret + "")
spark.conf.set("fs.azure.account.oauth2.client.endpoint." + storageAccountName + ".dfs.core.windows.net", "https://login.microsoftonline.com/" + tenantID + "/oauth2/token")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "true")
dbutils.fs.ls("abfss://" + fileSystemName  + "@" + storageAccountName + ".dfs.core.windows.net/")
spark.conf.set("fs.azure.createRemoteFileSystemDuringInitialization", "false")

// COMMAND ----------

// MAGIC %sh wget -P /tmp https://raw.githubusercontent.com/Azure/usql/master/Examples/Samples/Data/json/radiowebsite/small_radio_json.json

// COMMAND ----------

dbutils.fs.cp("file:///tmp/small_radio_json.json", "abfss://" + fileSystemName + "@" + storageAccountName + ".dfs.core.windows.net/")

// COMMAND ----------

val df = spark.read.json("abfss://" + fileSystemName + "@" + storageAccountName + ".dfs.core.windows.net/small_radio_json.json")

// COMMAND ----------

df.show()

// COMMAND ----------

val specificColumnsDf = df.select("firstname", "lastname", "gender", "location", "level")
specificColumnsDf.show()

// COMMAND ----------

val renamedColumnsDF = specificColumnsDf.withColumnRenamed("level", "subscription_type")
renamedColumnsDF.show()

// COMMAND ----------

val groupbycount = renamedColumnsDF.groupBy("gender","subscription_type").count()

groupbycount.show()


// COMMAND ----------

val blobStorage = "sudhirawmdwstorage.blob.core.windows.net"
val blobContainer = "adbcurated"
val blobAccessKey =  "sudhirawmdwstorage"

// COMMAND ----------

val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/tempDirs"

// COMMAND ----------

val acntInfo = "fs.azure.account.key."+ blobStorage
sc.hadoopConfiguration.set(acntInfo, blobAccessKey)

// COMMAND ----------

//SQL Data Warehouse related settings
val dwDatabase = "sudhirawsqlserver"
val dwServer = "sudhiraw-mdw-training"
val dwServerExtra=".database.windows.net"
val dwUser = "sudhiraw"
val dwPass = "P@ssw0rd20101"
val dwJdbcPort =  "1433"
val dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
val sqlDwUrl = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass + ";$dwJdbcExtraOptions"
val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass



// COMMAND ----------

spark.conf.set(
    "spark.sql.parquet.writeLegacyFormat",
    "true")

renamedColumnsDF.write.format("com.databricks.spark.sqldw")
.option("url", "jdbc:sqlserver://sudhiraw-mdw-training.database.windows.net:1433;database=sudhirawsqlserver;user=sudhiraw;password=P@ssw0rd20101")
.option("dbtable", "SampleTable")
.option( "forward_spark_azure_storage_credentials","True")
.option("tempdir", tempDir)
.mode("overwrite").save()

//jdbc:sqlserver://sudhiraw-mdw-training.database.windows.net:1433;database=sudhirawsqlserver;user={your_username_here};password={your_password_here};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;authentication=ActiveDirectoryPassword

//renamedColumnsDF.write
//  .format("com.databricks.spark.sqldw")
//  .option("url", "jdbc:sqlserver://sudhiraw-mdw-training.database.windows.net,1433;database=sudhirawsqlserver;user=sudhiraw@sudhiraw-mdw-training;password=P@ssw0rd20101;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;")
////  .option("forwardSparkAzureStorageCredentials", "true")
//  .option("dbTable", "my_table_in_dw_copy")
//  .option("tempDir", tempDir).mode("overwrite")
 // .save()



// COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.sudhirawmdwstorage.blob.core.windows.net",
  "jh/C0lpduGECFG/K9Y53A2ye2VRcyVdZhhpEy9YM/1Uf7qh2neRIdxbDCrORUiHyqx4F+pijj/N0R2SM8WHO6w==")

val df = spark.read
  .format("com.databricks.spark.sqldw")
  .option("url", "jdbc:sqlserver://sudhiraw-mdw-training.database.windows.net:1433;database=sudhirawsqlserver;user=sudhiraw;password=P@ssw0rd20101")
  .option("tempDir", tempDir)
  .option("forwardSparkAzureStorageCredentials", "true")
  .option("dbTable", "Ratings")
  .load()


// COMMAND ----------

df.show()

// COMMAND ----------

