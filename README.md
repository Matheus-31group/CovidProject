# Projeto de Engenharia de Dados - ETL de Dados da COVID-19

## Descrição do Projeto

Este projeto foi desenvolvido para uma agência governamental de dados com o objetivo de projetar e implementar um pipeline de ETL (Extract, Transform, Load) que processa os dados de COVID-19 disponíveis publicamente. Utilizei a plataforma **Databricks** para extrair, transformar, carregar e realizar análises sumárias dos dados, gerando insights sobre o impacto da pandemia por região.

Os dados utilizados são provenientes de [COVID-19 Open Data](https://storage.googleapis.com/covid19-open-data/v3/latest/aggregated.csv), uma base pública que fornece informações agregadas sobre casos de COVID-19.

## Arquitetura do Pipeline

### 1. Extração e Carga Bruta (Camada Bronze)

Na camada Bronze, os dados de COVID-19 são extraídos diretamente da URL pública [COVID-19 Open Data](https://storage.googleapis.com/covid19-open-data/v3/latest/aggregated.csv) utilizando a biblioteca `requests`. Após a requisição, o arquivo CSV é salvo temporariamente no sistema local e, em seguida, movido para o Databricks File System (DBFS) para processamento.

Os dados são então carregados diretamente em um DataFrame Spark e salvos no formato **Delta Lake** na camada Bronze. Essa camada mantém os dados em seu estado bruto, permitindo a retenção de todas as informações originais para consultas iniciais e futuras transformações.

### 2. Transformação (Camada Silver)

Na camada Silver, os dados são refinados para análises mais detalhadas. As transformações aplicadas incluem:
- **Seleção de colunas relevantes**: Apenas colunas essenciais, como `location_key`, `date`, `new_confirmed`, `new_deceased`, `cumulative_confirmed`, e outras relacionadas à análise, são mantidas.
- **Criação de novas colunas**: Uma nova coluna `year` é adicionada com base na coluna de datas, permitindo o particionamento dos dados por ano para consultas mais eficientes.

Essa camada garante que os dados estejam limpos e preparados para análises mais avançadas, otimizando a estrutura e o desempenho para consultas futuras.

### 3. Refinamento Final (Camada Gold)

Na camada Gold, os dados são refinados para finalidades de negócio e relatórios. As operações incluem:
- **Renomeação de colunas**: As colunas são renomeadas para termos mais descritivos e acessíveis, como `new_confirmed` para `daily_new_cases` e `cumulative_confirmed` para `total_confirmed_cases`.
- **Particionamento e otimização**: Os dados são particionados por ano e otimizados com a técnica **ZORDER** para melhorar a performance das consultas nas colunas mais utilizadas, como `location_key` e `date`.

A camada Gold entrega um conjunto de dados final altamente otimizado para análises, pronto para uso em dashboards e relatórios estratégicos.

### 4. Carga e Armazenamento

Os dados transformados em cada camada (Bronze, Silver, e Gold) são carregados no Databricks usando o formato **Delta Lake**, que oferece eficiência em consultas, suporte a versionamento e auditoria de alterações, além de escalabilidade para grandes volumes de dados.
