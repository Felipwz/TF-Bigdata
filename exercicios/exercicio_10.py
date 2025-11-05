# Exercício 10: Deletar um registro
# Remova uma linha da tabela usando DELETE FROM.

from pyspark.sql import SparkSession

# Inicializar Spark Session
spark = SparkSession.builder.appName("Exercicio10").getOrCreate()

# Mostrar dados antes da deleção
print("Dados antes da deleção:")
spark.sql("SELECT * FROM lab.db.pessoas").show()

# Deletar registro
spark.sql("DELETE FROM lab.db.pessoas WHERE id = 3")

# Mostrar dados após deleção
print("Dados após deleção:")
spark.sql("SELECT * FROM lab.db.pessoas").show()

spark.stop()
