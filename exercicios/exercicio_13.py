# Exercício 13: Consultar apenas um particionamento
# Leia somente as vendas do ano 2023.

from pyspark.sql import SparkSession

# Inicializar Spark Session
spark = SparkSession.builder.appName("Exercicio13").getOrCreate()

# Consultar apenas vendas de 2023
print("Vendas do ano 2023:")
spark.sql("SELECT * FROM lab.db.vendas WHERE ano = 2023").show()

# Alternativa usando DataFrame API
df = spark.table("lab.db.vendas")
vendas_2023 = df.filter(df.ano == 2023)
print("\nUsando DataFrame API:")
vendas_2023.show()

spark.stop()
