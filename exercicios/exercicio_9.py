# Exercício 9: Atualizar um registro
# Atualize um nome usando Iceberg SQL.

from pyspark.sql import SparkSession

# Inicializar Spark Session
spark = SparkSession.builder.appName("Exercicio9").getOrCreate()

# Mostrar dados antes da atualização
print("Dados antes da atualização:")
spark.sql("SELECT * FROM lab.db.pessoas").show()

# Atualizar registro
spark.sql("UPDATE lab.db.pessoas SET nome = 'Alice Silva' WHERE nome = 'Alice'")

# Mostrar dados após atualização
print("Dados após atualização:")
spark.sql("SELECT * FROM lab.db.pessoas").show()

spark.stop()
