# Exercício 15: Criar tabela Iceberg a partir de DataFrame
# Crie um DataFrame artificial e grave diretamente.

from pyspark.sql import SparkSession

# Inicializar Spark Session
spark = SparkSession.builder.appName("Exercicio15").getOrCreate()

# Criar DataFrame artificial
data = [
    (1, "Produto A", 50.00),
    (2, "Produto B", 75.50),
    (3, "Produto C", 120.00),
    (4, "Produto D", 35.99),
    (5, "Produto E", 200.00)
]

df = spark.createDataFrame(data, ["id", "produto", "preco"])

print("DataFrame criado:")
df.show()

# Gravar como tabela Iceberg
df.writeTo("lab.db.tabela_df").createOrReplace()

print("\nTabela criada com sucesso!")

# Verificar tabela criada
spark.sql("SELECT * FROM lab.db.tabela_df").show()

spark.stop()
