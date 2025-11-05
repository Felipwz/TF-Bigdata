# Exercício 7: Ler tabela Iceberg
# Faça uma query SELECT * FROM lab.db.pessoas e exiba o resultado.

from pyspark.sql import SparkSession

# Inicializar Spark Session
spark = SparkSession.builder.appName("Exercicio7").getOrCreate()

# Ler tabela Iceberg
df = spark.sql("SELECT * FROM lab.db.pessoas")

# Exibir resultado
df.show()

print(f"Total de colunas: {len(df.columns)}")
print(f"Colunas: {df.columns}")

spark.stop()
