# Projeto de Engenharia de Dados - ETL de Dados da COVID-19

Este projeto implementa um pipeline de ETL para processar dados de COVID-19 usando Python, SQL e Databricks. Ele segue a arquitetura Medallion (Bronze, Silver, Gold) para extrair, transformar e carregar os dados, proporcionando análises e insights a partir de uma base pública de dados dentro da plataforma do Databricks.

## Descrição do Projeto

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

## Implementação do Pipeline

O pipeline foi implementado no Databricks utilizando notebooks, seguindo a arquitetura Medallion, que é uma abordagem comum para gestão dos dados dentro da plataforma. A arquitetura organiza os dados, geralmente, em três camadas distintas:

- Camada Bronze: Armazena os dados brutos, exatamente como foram ingeridos.
- Camada Silver: Processa e limpa os dados, preparando-os para análise.
- Camada Gold: Contém os dados refinados e otimizados, prontos para uso em relatórios e análises de negócios.

Essas camadas variam de acordo com as necessidades do negócio. Como utilizei a versão Community do Databricks, algumas funcionalidades não estavam disponíveis, mas a execução do pipeline pode ser facilmente orquestrada com o Data Factory ou utilizando Jobs no Databricks.

Os notebooks que realizam a ingestão e o tratamento dos dados estão disponíveis na pasta "Data Ingest".


## Armazenamento na Plataforma Databricks
a. Escolha do Formato de Armazenamento
O formato de armazenamento escolhido para os dados transformados foi o Delta Lake, implementado em todas as camadas (Bronze, Silver e Gold). Esse formato foi utilizado para garantir a integridade e eficiência no processamento dos dados ao longo do pipeline ETL.

b. Justificativa do Formato e Suporte para Consultas
Optei por Delta Lake principalmente por suas características que atendem às demandas do pipeline implementado no Databricks:

- Escalabilidade
- Eficiência
- Controle


Por que não optei por outros formatos:

**Parquet**
Embora o Parquet ofereça armazenamento eficiente e compactado, não possui suporte nativo a transações ACID nem controle de versão. Isso limita sua capacidade de gerenciar atualizações de dados e auditorias ao longo do tempo. Aqui é importante destacar que ele pode ser uma ótima escolha para o projeto, mas dependendo da complexidade e necessidades, o delta é mais indicado.

**CSV**
O formato CSV é simples e legível, mas é ineficiente para grandes volumes de dados, além de não ser otimizado para consultas analíticas. Ele também não oferece compressão ou suporte a esquemas complexos.

**Avro**
Avro é uma boa opção para ingestão de dados em tempo real e dados de streaming, mas não é otimizado para análises em larga escala, que são fundamentais para a camada Gold do pipeline. 

Observação: os dados foram armazenados no DBFS especificamente para o caso desse projeto, mas uma opção é utilizar diretórios no Data Lake da Azure para armazenamento dos dados de cada camada, o que também pode ajudar na redução de custos.

## Análise de dados

A análise dos dados da covid foram realizadas em cima da camada gold e analisadas em SQL, conforme o notebook "Covid Analysis" na pasta **Notebooks**.

## Implementações de segurança e monitoramento

As recomendações de segurança e monitoramento estão descritas em dois notebooks "Estratégias de monitoramento" e "Implementação de medidas de segurança", na pasta **Notebooks**.

* Observações:

Análise de dados: Não consegui finalizar a análise dos dados como gostaria devido ao tempo limitado. Optei por focar nas partes mais desafiadoras tecnicamente para priorizar mostrar meu conhecimento em lógica e implementação. No ambiente de trabalho, entregaria sem problemas essas análises com a qualidade esperada. Embora a análise de dados seja algo que eu faço com facilidade, decidi priorizar a entrega das partes mais complexas. Se necessário, posso compartilhar uma análise de negócio similar que já fiz em Python, com insights estatísticos e visualizações.

Uso do Databricks Community: Como usei a versão Community do Databricks, parte da documentação está documentada e hospedada na plataforma que acabam não vindo para o Github.

Uso do IA: Em razão do tempo, utilizei como apoio apenas o chatGPT para me apoiar na escrita da documentação, gerando textos revisados a partir dos meus rascunhos. Cabe destacar que toda a solução, ideia e implementação foram minhas, baseado em experiência, pesquisa e lógica.

GitHub: Devido a imprevistos, acabei utilizando uma conta diferente do GitHub para este projeto. Algumas explicações podem ter ficado sem subir, mas são detalhes que posso esclarecer sem problemas.