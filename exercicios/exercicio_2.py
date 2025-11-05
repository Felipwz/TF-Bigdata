# Exercício 2: Salvar DataFrame no HDFS como CSV
# Crie um DataFrame e salve em hdfs://namenode:9000/data/ex1.csv no formato CSV.

from pyspark.sql import SparkSession

# Inicializar Spark Session
spark = SparkSession.builder.appName("Exercicio2").getOrCreate()

# Criar DataFrame
data = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Carlos")
]

df = spark.createDataFrame(data, ["id", "nome"])

# Salvar no HDFS como CSV
df.write.mode("overwrite").csv("hdfs://namenode:9000/data/ex1.csv", header=True)

print("DataFrame salvo em HDFS com sucesso!")

spark.stop()
