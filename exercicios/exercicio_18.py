# Exercício 18: Exportar tabela Iceberg para CSV
# Leia a tabela Iceberg e salve para HDFS.

from pyspark.sql import SparkSession

# Inicializar Spark Session
spark = SparkSession.builder.appName("Exercicio18").getOrCreate()

# Ler tabela Iceberg
df = spark.sql("SELECT * FROM lab.db.vendas")

print("Dados da tabela vendas:")
df.show()

# Exportar para CSV no HDFS
output_path = "hdfs://namenode:9000/export/vendas.csv"
df.write.mode("overwrite").csv(output_path, header=True)

print(f"\nTabela exportada para {output_path} com sucesso!")

# Verificar se o arquivo foi criado
print("\nLendo arquivo exportado:")
df_exported = spark.read.csv(output_path, header=True, inferSchema=True)
df_exported.show()

spark.stop()
