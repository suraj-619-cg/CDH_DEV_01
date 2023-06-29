#!/usr/bin/env python
# coding: utf-8

# In[1]:


#importing pyspark module
import pyspark
import findspark
findspark.find()
# In[3]:


#finding pyspark path
#findspark.init("C:\\Users\SURLANDE\PycharmProjects\spark_2.0\venv\Lib\site-packages\pyspark\bin")


# In[51]:


#importing sparkcontext & sparksession
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, ArrayType
arrayCol = ArrayType(StringType(),False)
from pyspark.sql.types import *
import pyspark.sql.functions as f


# In[17]:


#creating sparkcontext
sc=SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))


# In[19]:


#creating sparksession
spark=SparkSession.builder.appName("SPARK FIRST").config("spark.sql.legacy.ctePrecedence.enabled",True).getOrCreate()


# In[53]:


#Reading file into dataframe
df=spark.read.option("header","True").csv("C://Users/SURLANDE/njord-csv-sample.csv")


# In[54]:


#using add_month function
df3=df.withColumn("updated",add_months(df.ISODateTimeUTC,1))
#df2.orderBy(col("TWD").desc()).show()
df3.show()


# In[22]:


#function to match spark2 add_month function output with spark3

def add_months(start_date: str or Column, num_months: int):
    if isinstance(start_date, str):
        start_date = f.col(start_date)

    add_months_spark = f.add_months(start_date, num_months)
    start_date_is_last_day = f.last_day(start_date) == start_date

    return f.when(
        start_date_is_last_day,
        f.last_day(add_months_spark)
    ).otherwise(add_months_spark)


# In[23]:


df=spark.read.option("header","True").csv("C:/Users/SURLANDE/njord-csv-sample.csv")


# In[24]:


df.show(truncate=False)


# In[25]:


#matching add_month function output with spark2
df2=df.withColumn("updated",add_months(df.ISODateTimeUTC,1) )
#df2.orderBy(col("TWD").desc()).show()
df2.show()


# In[27]:


df2.printSchema()
window_spec=Window.orderBy(col("TWD").desc())

df2.withColumn("denserank",dense_rank().over(window_spec)).show()


# In[28]:


import time


# In[29]:


import time
start_time = time.time()
df2=df.withColumn("updated2",add_months(df.ISODateTimeUTC,1) )
print(f"Execution time: {time.time() - start_time}")


# In[30]:


#created external dataframe to check array function
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


# In[37]:


df.select("currentState").filter((df.currentState=="OH") & (df.previousState=="CA"))


# In[45]:


#using array function 
df_new=df.withColumn("output", array_contains(df["languagesAtSchool"],"Java"))


# In[46]:


df_new.show()


# In[ ]:


df.filter((df.currentState=="OH") & (df.previousState=="CA"))


# In[ ]:


df.show()


# In[55]:


#creating dataframe to check explode function
arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})]

df3 = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])



# In[56]:


#explode function
df3.select("knownLanguages",explode(df3.knownLanguages).alias("b")).show()


# In[ ]:




