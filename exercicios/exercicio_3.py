# Exercício 3: Ler CSV do HDFS
# Leia o arquivo salvo no exercício anterior usando spark.read.csv() e exiba o DataFrame.

from pyspark.sql import SparkSession

# Inicializar Spark Session
spark = SparkSession.builder.appName("Exercicio3").getOrCreate()

# Ler CSV do HDFS
df = spark.read.csv("hdfs://namenode:9000/data/ex1.csv", header=True, inferSchema=True)

# Exibir DataFrame
df.show()
df.printSchema()

spark.stop()
