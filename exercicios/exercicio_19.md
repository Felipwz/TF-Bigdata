# Exercício 19: Criar um Dashboard no Superset

## Objetivo
Adicionar o banco Trino no Superset, conectar ao catálogo Iceberg e criar uma visualização simples.

## Passos

### 1. Acessar o Superset
- Acesse o Superset através do navegador (geralmente em `http://localhost:8088`)
- Faça login com as credenciais configuradas

### 2. Adicionar conexão com Trino
1. Vá em **Data** → **Databases** → **+ Database**
2. Selecione **Trino** como tipo de banco
3. Configure a connection string:
   ```
   trino://trino@trino:8080/iceberg
   ```
4. Teste a conexão
5. Salve

### 3. Conectar ao catálogo Iceberg
1. Após adicionar o banco, vá em **Data** → **Datasets**
2. Clique em **+ Dataset**
3. Selecione:
   - **Database**: Trino (conexão criada)
   - **Schema**: lab.db
   - **Table**: pessoas ou vendas
4. Salve o dataset

### 4. Criar visualização
1. Vá em **Charts** → **+ Chart**
2. Selecione o dataset criado
3. Escolha um tipo de visualização (ex: Table, Bar Chart, etc.)
4. Configure:
   - **Metrics**: COUNT(*) ou SUM(valor)
   - **Dimensions**: ano, nome, etc.
5. Clique em **Run** para visualizar
6. Salve o chart

### 5. Criar Dashboard
1. Vá em **Dashboards** → **+ Dashboard**
2. Dê um nome ao dashboard
3. Adicione os charts criados
4. Organize o layout
5. Salve

## Exemplo de Query no Superset
```sql
SELECT 
    ano,
    COUNT(*) as total_vendas,
    SUM(valor) as valor_total
FROM lab.db.vendas
GROUP BY ano
ORDER BY ano
```

## Troubleshooting
- Verificar se o Trino está rodando: `docker ps | grep trino`
- Verificar logs do Trino: `docker logs trino`
- Verificar se as tabelas existem: acessar CLI do Trino e executar `SHOW TABLES IN iceberg.lab.db;`
