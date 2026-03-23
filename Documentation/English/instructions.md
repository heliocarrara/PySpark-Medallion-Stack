
---

### 1. Raiz do Projeto (`/`)
* **`docker-compose.yml`**: Define a infraestrutura. Deve subir um container de **Spark** (Bitnami é uma ótima escolha), um **Postgres** (para simular dados de cadastro de usuários) e o **Jupyter Lab** (para você codar no VS Code via browser ou extensão).
* **`README.md`**: É o seu cartão de visitas. Deve conter um diagrama da arquitetura (pode fazer no [Excalidraw](https://excalidraw.com/)), explicação da stack e instruções de como rodar o `docker-compose`.
* **`.gitignore`**: Essencial para não subir lixo, pastas `__pycache__` ou os dados pesados da pasta `data_lake/` (suba apenas a estrutura de pastas, não os milhares de JSONs).

---

### 2. `01_source_simulation/` (A Origem)
Aqui você simula o sistema que gera o dinheiro/dados para a empresa.
* **`producer.py`**: Script que gera o JSON da transação de XLM. Como você quer simular a API, use a biblioteca `random` para flutuar o preço entre **0.10 e 0.15** e gerar volumes aleatórios.
* **Lógica de Negócio**: O script deve salvar o arquivo na pasta `data_lake/landing/`. Para ser realista, salve com o nome: `xlm_v1_TIMESTAMP.json`.

---

### 3. `02_ingestion_bronze/` (O "Fake" Data Factory)
O Azure Data Factory (ADF) move dados. Aqui você mostra que entende de **Metadados**.
* **`ingest_to_bronze.py`**: Este script lê da `landing/` e move para a `bronze/`.
* **O Diferencial**: Ao mover o dado, o script deve criar pastas por data (particionamento): `/bronze/year=2026/month=03/day=23/`. Isso é o que o ADF faz por baixo dos panos para otimizar a performance.

---

### 4. `03_processing_silver/` (O Reino do PySpark)
Aqui você brilha no código de engenharia pesado.
* **`transform_silver.ipynb`**: Um notebook que lê os JSONs da Bronze usando **PySpark**.
* **O que fazer aqui**: 
    * Definir o `Schema` (não deixe o Spark adivinhar, defina os tipos explicitamente).
    * Tratar nulos (se o preço vier nulo, descarta a linha ou preenche com a média).
    * **Delta Lake**: Salve o resultado final em formato `.delta`. O Delta permite que você dê um "UPDATE" ou "DELETE" em dados do Lake, algo que a vaga de Databricks exige que você saiba.

---

### 5. `04_analytics_gold/` (Modelagem SQL)
Aqui você prepara o banquete para os analistas de negócios.
* **`model_gold_tables.sql`**: Queries que pegam a tabela Silver e transformam em tabelas de negócio.
* **O que fazer aqui**: 
    * **Agregações**: Preço médio por hora, volume total por dia.
    * **Join**: Cruzar a transação com a tabela de usuários (que está no seu Postgres do Docker) para saber de qual país veio a compra.
    * O resultado final deve ser salvo na pasta `gold/` (também em Delta ou Parquet).

---

### 6. `05_dashboard_pbi/` (Visualização)
* **`xlm_dashboard.pbix`**: O arquivo final.
* **Dica Profissional**: Como você usou Docker, no Power BI você vai selecionar "Obter Dados" -> "Pasta" e apontar para a sua pasta `data_lake/gold/`. Quando novos dados chegarem na Gold via Spark, o Power BI atualizará no clique.

---

### 7. `data_lake/` (O Armazenamento)
Esta pasta simula o seu **Azure Data Lake Gen2**. Ela deve ser organizada para que qualquer ferramenta saiba onde achar o quê.
* **Landing**: Dados brutos e desorganizados.
* **Bronze**: Dados originais, mas particionados por data.
* **Silver**: Dados limpos, tipados e em formato Delta.
* **Gold**: Tabelas prontas para o BI (Star Schema).

---
