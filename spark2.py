#!/usr/bin/env python
# coding: utf-8

# In[37]:


import pyspark
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.functions import *
from pyspark.sql.functions import desc
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StringType, ArrayType
arrayCol = ArrayType(StringType(),False)
from pyspark.sql.types import *
import pandas as pd


# In[40]:



import pdfkit


# In[10]:


import findspark
findspark.init()
findspark.find()


# In[11]:


print(pyspark.__version__)


# In[14]:


from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[*]').appName("spark 2.4").getOrCreate()


# In[15]:


spark


# In[16]:


df = spark.read.option("inferschema",True).option("header",True).csv("C://Users/SURLANDE/PycharmProjects/spark_2.0/people-100.csv")


# In[17]:


df.printSchema()


# In[ ]:





# In[18]:


df.select(col("Date of birth"),month(col("Date of birth")).alias("month"),current_timestamp().alias("current_timestamp")).show()


# In[19]:


df_addmonth = df.withColumn('Date of birth_addmonth', F.add_months(df['Date of birth'], 1))
new_df=df_addmonth.select(col("Date of birth"),month(col("Date of birth")).alias("month"),dayofmonth(col("Date of birth")).alias("date"),col("Date of birth_addmonth"),month(col("Date of birth_addmonth")).alias("added_month+1"),dayofmonth(col("Date of birth_addmonth")).alias("date_0"),current_timestamp().alias("current_timestamp"))


# In[20]:


new_df = new_df.withColumn("index", monotonically_increasing_id())
new_df.orderBy(desc("index")).drop('index').show(10)


# In[21]:


new_df.select(array(col("month"),col("added_month+1"))).show(truncate=False)


# In[54]:


df_demo_array=spark.read.option("inferschema",True).option("header",True).csv("C://Users/SURLANDE/demo_array_data.csv")


# In[55]:


df_demo_array.printSchema()


# In[56]:


df_new = df_demo_array.withColumn("knownLanguages", array(df_demo_array["knownLanguages"])).withColumn("properties", array(df_demo_array["properties"]))


# In[57]:


df_new.printSchema()


# In[58]:


df_new.show()


# In[59]:


from pyspark.sql.functions import explode
df_new.select("name",explode("knownLanguages")).show()


# In[ ]:





# In[ ]:


# array function:-
data = [
("James,,Smith",["Java","Scala","C++"],["Spark","Java"],"OH","CA"),
("Michael,Rose,",["Spark","Java","C++"],["Spark","Java"],"NY","NJ"),
("Robert,,Williams",["CSharp","VB"],["Spark","Python"],"UT","NV")
]
from pyspark.sql.types import StringType, ArrayType,StructType,StructField
schema = StructType([ 
    StructField("name",StringType(),True), 
    StructField("languagesAtSchool",ArrayType(StringType()),True), 
    StructField("languagesAtWork",ArrayType(StringType()),True), 
    StructField("currentState", StringType(), True), 
    StructField("previousState", StringType(), True)
  ])
df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show()
# timestamp function :-
df.select("currentState").filter((df.currentState=="OH") & (df.previousState=="CA"))
df_new=df.withColumn("output", array_contains(df["languagesAtSchool"],"Java"))
df_new.show()
#expplode function:-
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()
arrayData = [

        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),

        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),

        ('Robert',['CSharp',''],{'hair':'red','eye':''}),

        ('Washington',None,None),

        ('Jefferson',['1','2'],{})]
df3 = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
df3.select("knownLanguages",explode(df3.knownLanguages).alias("b")).show()

