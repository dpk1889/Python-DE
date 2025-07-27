# Databricks notebook source
#spark 
print("gonna make it to agentic ai developer")

# COMMAND ----------

#spark open src,distributed computing engine 
#in memory and parallel processing -> 100x in memory and 10x in disk 
#layered architecture and master slave concept
#driver->cluster manager->worker = loosely coupled 
#driver talks to cluster manager for resources and driver node talks directly to worker nodes 


# COMMAND ----------

#n executors in one worker node 
#no of partitions equals to multiple of number of cores in executors 
#user submit app->driver initiates spark session->dag creates logical plan->task executors req for resources from cluster manager -> cluster manager allocates resources-> driver directly connect with workers -> worker executres the task and
#  return to driver -> driver return to user - end of spark process 


# COMMAND ----------

# spark is scalable,fault tolernat,polyglot,real time,speed and rich libraries 
# kafka,eventhub for streaming
#driver and worker = jvm processes , each executor has its own jvm process
#application= piece of code that runs on spark
#stage= no of times we shuffle data for transformations 
#stages are further divided into multiple tasks , each task will have same logic-> each task per partition
#job- stage 1 to stage n=> tasks 1 to tasks n 
#transformation imput rdd and creates new rdd until action is called 
#dag tracks all transformation and lineage graph is maintained 
#action data output is need to store for user
#rdd reilient distributed dataset is basic data sttucture of spark. when spark reads or creates or data 
# ir creates rdd distributed across nodes in the form of partition

# COMMAND ----------

#ececutor- each worker node has mutiple executors
#partitions - rdd/dataframe is stored in memory cluster in the form of partition
#core each executor can consists of multiple core configurable by settings
# on heap memory = executor memory lies within jvm process mamanged jvm
#off heap= executor memory lies outside jvm process managed by os



# COMMAND ----------

# libraries - sql, streaming,mllob,graphx
#languages= scala,java,python,r
#driver initiate spark session -> spark session creates spark context -> spark context creates spark conf -> spark conf creates spark conf object -> spark conf object creates spark conf file -> spark conf
#dag -> task scheduler -> deploy scheduler  worker nodes -> output tracker 

# COMMAND ----------

#spark apis - rdds,dataframe,datasets 
#rdd- oops,type safety( compile time error)
#dataframe- relational format, optimised, memory managament - on and off heap memory 
#interview questions from here 

# COMMAND ----------

#transformation - lazy evaluation based on dag -> filter,union 
#action display or store output => count,collect,save 
#narrow transformation = shuffled data and expensive 
#wide transformations - groupby => dept -it,finance,sales 

# COMMAND ----------

if dbutils.fs.ls('/Volumes/workspace/default/de_practice'):
    print("Path exists")
else:
    print("Path does not exist")

# COMMAND ----------

dbutils.fs.ls('/Volumes/workspace/default/de_practice')

# COMMAND ----------

df=spark.read.format("csv").option("inferSchema",True).option("header",True).load('/Volumes/workspace/default/de_practice')
df.show(4)



# COMMAND ----------

df.display(4)

# COMMAND ----------

dfind=df.filter(df.Country=="India").display()
dfpak=df.filter(df.Country=="Pakistan").display()

# COMMAND ----------

dfind = df.filter(df.Country == "India")
dfpak = df.filter(df.Country == "Pakistan")

display(dfind)
display(dfpak)

dfasia = dfind.unionAll(dfpak)
display(dfasia)

dfasia_count = dfasia.count()

# COMMAND ----------

# Ensure df1 and df2 are properly initialized as DataFrames
df1 = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
df2 = spark.createDataFrame([(3, "c"), (4, "d")], ["id", "value"])

# Perform the union operation
df3 = df1.union(df2)
display(df3)

# COMMAND ----------

dfgroup=df.groupBy("country").count()
dfgroup.show()

# COMMAND ----------

dforig=spark.read.format("csv").option("inferSchema",True).option("header",True).load('/Volumes/workspace/default/de_practice')
dforig.show(4)



# COMMAND ----------

dforig.display()

# COMMAND ----------

dfgroupbycountry=dforig.groupBy("country").count()
dfgroupbycountry.show()
#dfgroupbycountry.display()

# COMMAND ----------

dfgroupbycountry.collect()

# COMMAND ----------

#cluster is computing infrsrstucture = set of computation resources and configurations = executes de, ds worloads 
#all purpose = interactive = terminated,restarted manually 
#job cluster=fast automated jobs
#pools= attaching cluster to reeduce boot time and auto scaling = instances always ready 
#standard= single user, high concurrency=max resources utilization and minimum query latency, suitable for collaboration
#single node= no worker node provisioned 
#cluster runtime- core components needed to run cluster- differs in usability,performanace and security
#memory optimised,storage optimised,general purpose


# COMMAND ----------

# #reading csv files
# #df=spark.read.format(file_type)\
#     .option("inferSchema", value)\
#     .option("header", value)\
#     .option("sep", value)
#     .schema(schema)
#     .load(file_location)
# we can options and write all in one
# formats=csv,parquet,orc,json,avro
# options=inferSchema,header,delimiter
# schema=define,schema,use schema
# file= single,multiple and folder 

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType
schema=StructType([StructField("Index",IntegerType(),True),
                   StructField("Organization Id",StringType(),True),
                   StructField("Name",StringType(),True),
                   StructField("Website",StringType(),True),
                   StructField("Country",StringType(),True),
                   StructField("Description",StringType(),True),
                   StructField("Founded",IntegerType(),True),
                   StructField("Industry",StringType(),True),
                   StructField("Number of employees",IntegerType(),True)])
dfschemadefined=spark.read.format("csv").option("header",True).schema(schema).load("/Volumes/workspace/default/de_practice")
dfschemadefined.head(4)

# COMMAND ----------

# schema_alternate = 'Index INT,Organization_Id STRING,Name STRING,Website STRING,Country STRING,Description STRING,Founded INTEGER,Industry STRING,Number_of_employees INTEGER'
# dfschemaalt = spark.read.format("csv").option("header", True).schema(schema_alternate).load("/Volumes/workspace/default/de_practice")
# dfschemaalt.display()
# dfschemaalt.printSchema()

# COMMAND ----------

#FILTER CONDITIONS 
dforig.printSchema()


# COMMAND ----------

dffilter1=dforig.filter(dforig["Country"]=="Oman")
dffilter1.display()
dffilter2=dforig.filter(dforig["Number of employees"]>=2000)
dffilter2.display()
dffilter3=dforig.filter(dforig["Founded"]<1980)
dffilter3.display()
dffilter4=dforig.filter((dforig["Country"]=="Oman" )& (dforig["Number of employees"]>=2000) & (dforig["Founded"]<1980))
dffilter4.display()

# COMMAND ----------

dffilter4=dforig.filter((dforig["Country"]=="Oman") & (dforig["Number of employees"]>=2000) & (dforig["Founded"]<1980))
dffilter4.display()

# COMMAND ----------

display(dforig.filter(dforig.Name.startswith("Ri")))
display(dforig.filter(dforig.Name.endswith("ly")))
display(dforig.filter(dforig.Name.contains("and")))

# COMMAND ----------

display(dforig.filter(dforig.Name.isNull()))
display(dforig.filter(dforig.Name.isNotNull()))
display(dforig.filter(dforig.Name.like("%and%")))

# COMMAND ----------

display(dforig.filter(dforig.Founded.isin(1980,1999)))


# COMMAND ----------

display(dforig.filter(dforig["Number of employees"].between(1000,2000)))
display(dforig.filter(dforig.Name.like("%der%")))
#GROUP BY


# COMMAND ----------

#df.withRenamedColumn("oldcol","newcol")
dforig.withColumnRenamed('Founded','Year_Founded').display()
dforig.withColumn('Year_Founded_new',dforig['Founded']+10000).display()
dforig.drop("Year_Founded_new")

# COMMAND ----------

dforig=spark.read.format("csv").option("inferSchema",True).option("header",True).load('/Volumes/workspace/default/de_practice')
dforig.display(4)



# COMMAND ----------

dforig.display()
dforig.groupBy("Country").count().display()
dforig.groupBy("Country").count().orderBy("count").display()

# COMMAND ----------

display(dforig)

# COMMAND ----------

from pyspark.sql.functions import concat,lit
dffullname=dforig.withColumn("Full_Name",concat(dforig.Name,lit(" "),dforig.Website))
dffullname.display()

# COMMAND ----------

dffullnamedrop=dffullname.drop("Full_Name")
dffullnamedrop.display()
#dforig.groupBy("Country").count().display()

# COMMAND ----------

#dataframe join methods - inner, left, right, outer, leftanti, leftsemi
from pyspark.sql.functions import concat,lit
dffullname=dforig.withColumn("Full_Name",concat(dforig.Name,lit(" "),dforig.Website))
dffullnamejoined=dffullname.join(dforig,["Name"],"inner")
dffullnamejoinedleftanti=dffullname.join(dforig,["Name"],"left_anti")
dffullnamejoinedleftsemi=dffullname.join(dforig,["Name"],"left_semi")
dffullnamejoinedright=dffullname.join(dforig,["Name"],"right")


dffullnamejoinedright.display()
dffullnamejoinedleftanti.display()

# COMMAND ----------

#left semi join =inner join - returns only the rows from the left table that have matching rows in the right table,common record from left table 
#left anti join =anti join - returns only the rows from the left table that do not have matching rows in the right table,uncommon records from table 

# COMMAND ----------

#databricks utility commands - input ,output parameters,retrieve secrets from azure key vaults,dbfs and notebook help commands
#dbutils.fs.help()
#dbutils.fs.ls("dbfs:/FileStore/tables")
#dbutils.fs.put("dbfs:/FileStore/tables/test.txt","This is a test file",True)
#dbutils.fs.head("dbfs:/FileStore/tables/test.txt")
dbutils.fs.help()
dbutils.widgets.help()  
dbutils.secrets.help()


# COMMAND ----------

# Create the volume first
spark.sql("""
CREATE VOLUME IF NOT EXISTS workspace.default.de_practice_copy
COMMENT 'This is my example managed volume'
""")

# Now create the directory
dbutils.fs.mkdirs("/Volumes/workspace/default/de_practice_copy")

# COMMAND ----------



# COMMAND ----------

dbutils.fs.cp("/Volumes/workspace/default/de_practice", "/Volumes/workspace/default/de_practice_copy", recurse=True)
dbutils.fs.ls("/Volumes/workspace/default/de_practice_copy")
dbutils.fs.head("/Volumes/workspace/default/de_practice_copy/organizations-10000.csv")
#dbutils.fs.rm("/Volumes/workspace/default/de_practice_copy", recurse=True)

# COMMAND ----------

