# Exercício 6: Inserir dados na tabela Iceberg
# Insira 3 valores manualmente usando SQL INSERT INTO.

from pyspark.sql import SparkSession

# Inicializar Spark Session
spark = SparkSession.builder.appName("Exercicio6").getOrCreate()

# Inserir dados na tabela
spark.sql("INSERT INTO lab.db.pessoas VALUES (1, 'Alice')")
spark.sql("INSERT INTO lab.db.pessoas VALUES (2, 'Bob')")
spark.sql("INSERT INTO lab.db.pessoas VALUES (3, 'Carlos')")

print("Dados inseridos com sucesso!")

# Verificar dados inseridos
spark.sql("SELECT * FROM lab.db.pessoas").show()

spark.stop()
