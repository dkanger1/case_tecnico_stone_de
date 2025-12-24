Segue desenho da solução em anexo.

Desenho da Solução
A arquitetura proposta segue o padrão de camadas STAGING(RAW) -> BRONZE -> SILVER -> GOLD no BigQuery.

A ingestão dos arquivos CSV é orquestrada pelo Airflow( ou Cloud Composer), para mover os dados da fonte para uma camada de staging no Cloud Storage, garantindo versionamento e possibilidade de reprocessamento.

A carga da camada Staging para Bronze é realizada via BigQuery Load Jobs, mantendo os dados tipados, particionados pela data de processamento e com validações básicas de qualidade.

As camadas Silver e Gold são construídas utilizando Dataform ou DBT, enquanto o Airflow atua como orquestrador da execução. A utilização dessas ferramentas permite melhor governança, versionamento das regras de negócio, data quality, linhagem automática e reprocessamento controlado.

Backfill:
O backfill é viabilizado através do modelo incremental por data nas camadas Silver e Gold, que utilizando filtros por data executa as operações de INSERT e MERGE (somente na parte afetada).

O uso de dbt ou Dataform facilita esse processo ao fornecer controle de dependências, linhagem dos dados e execução incremental, reduzindo custo e tempo de processamento.