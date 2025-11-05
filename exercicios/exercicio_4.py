# Exercício 4: Criar namespace Iceberg
# Crie um namespace chamado lab.db no catálogo Iceberg.

from pyspark.sql import SparkSession

# Inicializar Spark Session com configurações Iceberg
spark = SparkSession.builder.appName("Exercicio4").getOrCreate()

# Criar namespace
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.db")

print("Namespace lab.db criado com sucesso!")

# Verificar namespaces existentes
spark.sql("SHOW NAMESPACES").show()

spark.stop()
