# Exercício 12: Inserir dados particionados
# Insira dados variando o valor de ano.

from pyspark.sql import SparkSession

# Inicializar Spark Session
spark = SparkSession.builder.appName("Exercicio12").getOrCreate()

# Inserir dados de diferentes anos
spark.sql("INSERT INTO lab.db.vendas VALUES (1, 100.50, 2022)")
spark.sql("INSERT INTO lab.db.vendas VALUES (2, 250.75, 2022)")
spark.sql("INSERT INTO lab.db.vendas VALUES (3, 150.00, 2023)")
spark.sql("INSERT INTO lab.db.vendas VALUES (4, 300.25, 2023)")
spark.sql("INSERT INTO lab.db.vendas VALUES (5, 450.80, 2024)")

print("Dados inseridos com sucesso!")

# Verificar dados inseridos
spark.sql("SELECT * FROM lab.db.vendas ORDER BY ano, id").show()

spark.stop()
