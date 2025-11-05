# Exercício 16: Converter tabela para Iceberg
# Crie uma tabela Parquet simples e converta para Iceberg.

from pyspark.sql import SparkSession

# Inicializar Spark Session
spark = SparkSession.builder.appName("Exercicio16").getOrCreate()

# Criar DataFrame
data = [
    (1, "Item A"),
    (2, "Item B"),
    (3, "Item C")
]

df = spark.createDataFrame(data, ["id", "descricao"])

# Salvar como tabela Parquet
df.write.mode("overwrite").saveAsTable("lab.db.tabela_parquet", format="parquet")

print("Tabela Parquet criada!")

# Verificar formato original
spark.sql("DESCRIBE EXTENDED lab.db.tabela_parquet").show(truncate=False)

# Converter para Iceberg (pode variar dependendo da configuração)
# Nota: A conversão direta pode não estar disponível em todas as versões
# Uma alternativa é recriar a tabela como Iceberg
print("\nConvertendo para Iceberg...")

# Ler dados da tabela Parquet
df_parquet = spark.table("lab.db.tabela_parquet")

# Criar nova tabela Iceberg com os mesmos dados
df_parquet.writeTo("lab.db.tabela_iceberg_convertida").createOrReplace()

print("Tabela convertida para Iceberg!")

# Verificar nova tabela
spark.sql("SELECT * FROM lab.db.tabela_iceberg_convertida").show()

spark.stop()
