# Exercício 1: Criar um DataFrame simples
# Crie um DataFrame com três linhas e duas colunas (id, nome) e mostre seu conteúdo.

from pyspark.sql import SparkSession

# Inicializar Spark Session
spark = SparkSession.builder.appName("Exercicio1").getOrCreate()

# Criar DataFrame
data = [
    (1, "Alice"),
    (2, "Bob"),
    (3, "Carlos")
]

df = spark.createDataFrame(data, ["id", "nome"])

# Mostrar conteúdo
df.show()

spark.stop()
