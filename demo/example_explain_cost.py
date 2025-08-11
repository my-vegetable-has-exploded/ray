import pyspark
from pyspark.sql import SparkSession
import time

# 创建 SparkSession
spark = SparkSession.builder.appName("explain_cost_example").getOrCreate()

# 创建一个简单的 DataFrame
print("创建 DataFrame...")
df = spark.createDataFrame([(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"])
print("DataFrame 创建完成")

# 添加一个自定义函数来检测是否被执行
def slow_function(x):
    # 模拟耗时操作
    time.sleep(20)
    print("slow_function 被执行!")
    return x.upper()

# 注册为 UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
slow_udf = udf(slow_function, StringType())

print("\n应用转换操作...")
# 应用转换操作，但不触发执行
df_transformed = df.withColumn("name_upper", slow_udf(df["name"]))
print("转换操作应用完成，但尚未执行")

print("\n调用 explain(mode='cost')...")
# 调用 explain(mode="cost") 查看是否触发执行
df_transformed.explain(mode="cost")
print("explain(mode='cost') 完成")

print("\n现在显式触发执行...")
# 显式触发执行
result = df_transformed.collect()
print("执行完成，结果:")
for row in result:
    print(row)

# 停止 SparkSession
spark.stop()