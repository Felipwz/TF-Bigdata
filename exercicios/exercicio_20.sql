-- Exercício 20: Ler tabela Iceberg via Trino
-- No Superset ou CLI do Trino

-- Conectar ao catálogo Iceberg
USE iceberg;

-- Listar schemas disponíveis
SHOW SCHEMAS;

-- Usar o schema lab.db
USE iceberg.lab.db;

-- Listar tabelas no schema
SHOW TABLES;

-- Ler tabela pessoas
SELECT * FROM iceberg.lab.db.pessoas;

-- Ler tabela vendas
SELECT * FROM iceberg.lab.db.vendas;

-- Query com agregação
SELECT 
    ano,
    COUNT(*) as total_vendas,
    SUM(valor) as valor_total,
    AVG(valor) as valor_medio,
    MIN(valor) as valor_minimo,
    MAX(valor) as valor_maximo
FROM iceberg.lab.db.vendas
GROUP BY ano
ORDER BY ano;

-- Query com filtro
SELECT * 
FROM iceberg.lab.db.vendas 
WHERE valor > 100
ORDER BY valor DESC;

-- Verificar metadados da tabela
DESCRIBE iceberg.lab.db.vendas;

-- Ver propriedades da tabela
SHOW CREATE TABLE iceberg.lab.db.vendas;
