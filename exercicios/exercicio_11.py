# Exercício 11: Criar tabela particionada
# Crie uma tabela Iceberg com partição por ano.

from pyspark.sql import SparkSession

# Inicializar Spark Session
spark = SparkSession.builder.appName("Exercicio11").getOrCreate()

# Criar tabela particionada
spark.sql("""
    CREATE TABLE IF NOT EXISTS lab.db.vendas (
        id INT,
        valor DOUBLE,
        ano INT
    ) USING ICEBERG
    PARTITIONED BY (ano)
""")

print("Tabela lab.db.vendas criada com sucesso!")

# Verificar detalhes da tabela
spark.sql("DESCRIBE EXTENDED lab.db.vendas").show(truncate=False)

spark.stop()
