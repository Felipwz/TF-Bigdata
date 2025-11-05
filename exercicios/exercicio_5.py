# Exercício 5: Criar tabela Iceberg
# Crie uma tabela Iceberg lab.db.pessoas (id INT, nome STRING) usando SQL.

from pyspark.sql import SparkSession

# Inicializar Spark Session
spark = SparkSession.builder.appName("Exercicio5").getOrCreate()

# Criar tabela Iceberg
spark.sql("""
    CREATE TABLE IF NOT EXISTS lab.db.pessoas (
        id INT,
        nome STRING
    ) USING ICEBERG
""")

print("Tabela lab.db.pessoas criada com sucesso!")

# Verificar tabelas no namespace
spark.sql("SHOW TABLES IN lab.db").show()

spark.stop()
