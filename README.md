# Credit-Pipeline

🚀 Construindo um Projeto Real de Engenharia de Dados (Foco em Crédito)

Nas últimas semanas venho estruturando um projeto de Engenharia de Dados com foco em me preparar para atuar como Engenheiro de Dados Pleno na área de crédito.

O objetivo não era apenas “rodar PySpark”, mas construir algo com arquitetura próxima de ambiente produtivo.

📊 Dataset

Utilizei o Brazilian E-Commerce Public Dataset by Olist como base transacional.

🏗 Arquitetura Implementada

Estruturei o projeto como um Data Lake com camadas:

Landing → Bronze → Silver → Gold

🥉 Bronze

Conversão de CSV para Parquet

Schema explícito (sem inferSchema)

Colunas técnicas (ingestion_timestamp, source)

Estrutura pronta para auditoria e reprocessamento

🥈 Silver

Tipagem correta

Conversão de datas

Normalização de strings

Deduplicação

Dados confiáveis para consumo analítico

🥇 Gold

Criação de modelo dimensional (Star Schema):

dim_customers

dim_date

fact_orders

Uso de surrogate keys e separação clara entre fatos e dimensões.

📈 Evolução: Camada Analítica com Métricas

Além do modelo dimensional, evoluí o projeto para gerar uma tabela analítica com indicadores financeiros, como:

Taxa de cancelamento

Volume de pedidos por estado

Tempo médio entre compra e aprovação

Frequência de clientes recorrentes

Indicadores de comportamento transacional

Essa camada já permite alimentar dashboards ou análises estratégicas.

🧠 Boas práticas aplicadas

✔ Estrutura modular (jobs/, schemas/, utils/)
✔ Separação clara de responsabilidades
✔ Pipeline idempotente
✔ Controle de schema
✔ Organização pronta para orquestração
✔ Preparado para evolução para ingestão incremental e Delta Lake

⚙ Desafios resolvidos

Configuração do Spark no Windows (winutils.exe)

Estruturação de projeto Python como pacote

Organização de imports e modularização

Pensar performance (shuffle, partições, parquet vs csv)

🎯 Principal aprendizado

Engenharia de Dados não é só transformar dados.

É pensar em:

Escalabilidade

Governança

Reprocessamento

Performance

Modelo analítico

Valor de negócio
