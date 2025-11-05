# Exercício 8: Contar registros
# Conte quantos registros existem na tabela lab.db.pessoas.

from pyspark.sql import SparkSession

# Inicializar Spark Session
spark = SparkSession.builder.appName("Exercicio8").getOrCreate()

# Contar registros
count = spark.sql("SELECT COUNT(*) as total FROM lab.db.pessoas")
count.show()

# Alternativa usando DataFrame API
df = spark.table("lab.db.pessoas")
total = df.count()
print(f"Total de registros: {total}")

spark.stop()
