# Exercício 14: Ver metadados da tabela
# Use DESCRIBE HISTORY e DESCRIBE DETAIL para ver metadados.

from pyspark.sql import SparkSession

# Inicializar Spark Session
spark = SparkSession.builder.appName("Exercicio14").getOrCreate()

# Ver histórico da tabela
print("Histórico da tabela vendas:")
spark.sql("DESCRIBE HISTORY lab.db.vendas").show(truncate=False)

print("\n" + "="*80 + "\n")

# Ver detalhes da tabela
print("Detalhes da tabela vendas:")
spark.sql("DESCRIBE DETAIL lab.db.vendas").show(truncate=False)

print("\n" + "="*80 + "\n")

# Informações estendidas
print("Informações estendidas:")
spark.sql("DESCRIBE EXTENDED lab.db.vendas").show(truncate=False)

spark.stop()
