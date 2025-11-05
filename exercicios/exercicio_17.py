# Exercício 17: Leitura incremental (Time Travel)
# Volte para uma versão anterior da tabela.

from pyspark.sql import SparkSession

# Inicializar Spark Session
spark = SparkSession.builder.appName("Exercicio17").getOrCreate()

# Mostrar histórico da tabela
print("Histórico da tabela vendas:")
spark.sql("DESCRIBE HISTORY lab.db.vendas").show(truncate=False)

print("\n" + "="*80 + "\n")

# Ler versão atual
print("Dados da versão atual:")
spark.sql("SELECT * FROM lab.db.vendas").show()

print("\n" + "="*80 + "\n")

# Ler versão específica (versão 1) - Time Travel
print("Dados da versão 1 (Time Travel):")
spark.sql("SELECT * FROM lab.db.vendas VERSION AS OF 1").show()

# Alternativa: Ler por timestamp
# spark.sql("SELECT * FROM lab.db.vendas TIMESTAMP AS OF '2024-01-01 00:00:00'").show()

spark.stop()
