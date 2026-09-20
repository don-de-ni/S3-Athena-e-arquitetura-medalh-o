Pipeline de Dados de Vendas — Python, Amazon S3 e Athena
1. Objetivo
Este projeto implementa um pipeline de dados de vendas utilizando Python e a arquitetura Medallion, com as camadas Raw, Silver e Gold.

O pipeline realiza a geração de dados simulados, a validação de regras de qualidade, a segregação de registros inválidos em quarentena, o enriquecimento dos pedidos válidos e a criação de indicadores analíticos.

O processamento é executado localmente. Os arquivos resultantes são publicados no Amazon S3 e consultados pelo Amazon Athena, utilizando tabelas externas registradas no AWS Glue Data Catalog.

2. Tecnologias utilizadas
Python: geração e processamento dos dados.
pandas: leitura, validação, joins e agregações.
PyArrow: gravação dos arquivos Parquet com compressão Snappy.
Amazon S3: armazenamento das camadas.
AWS Glue Data Catalog: catálogo das tabelas externas.
Amazon Athena: consultas SQL e auditoria dos dados.
AWS CLI: opção para envio dos arquivos ao S3.
3. Fluxo do pipeline
gerar_base.py
      │
      ▼
Raw: clientes.csv, produtos.csv e pedidos.csv
      │
      ▼
pipeline.py
      │
      ▼
Validação de qualidade
      │
      ├── Pedidos inválidos
      │         │
      │         ▼
      │     Quarentena — JSON Lines
      │
      └── Pedidos válidos
                │
                ▼
       JOIN com clientes e produtos
                │
                ▼
       Cálculo de valor_total
                │
                ▼
       Silver — Parquet/Snappy
                │
                ▼
       Agregação por UF e categoria
                │
                ▼
       Gold — Parquet/Snappy
                │
                ▼
       Conciliação dos resultados
Antes da gravação das saídas, o pipeline verifica a consistência entre os registros de entrada, os pedidos tratados, a quarentena e os indicadores agregados.

4. Arquivos do projeto
gerar_base.py
Gera três arquivos CSV:

Arquivo	Conteúdo	Quantidade
clientes.csv	Cadastro fictício de clientes	20
produtos.csv	Catálogo de produtos e preços	12
pedidos.csv	Pedidos válidos e inválidos	100
A geração utiliza uma semente fixa para permitir a reprodução dos dados, mantendo a mesma data e a mesma versão do código.

O script também cria resultado_esperado.json, que funciona como gabarito para conferência. Esse arquivo não é utilizado pelo pipeline para decidir quais pedidos são válidos.

pipeline.py
Lê os arquivos CSV da estrutura local, aplica as regras de qualidade e produz:

pedidos rejeitados em JSON Lines;
fato de vendas enriquecida em Parquet/Snappy;
indicadores por UF e categoria em Parquet/Snappy;
resumo da execução e resultados de conciliação no terminal.
O processamento não modifica os arquivos originais da Raw.

5. Estrutura dos dados
Clientes
cliente_id, nome, email, uf
Produtos
product_id, nome_produto, categoria, preco
Pedidos
pedido_id, cliente_id, product_id, quantidade, data_pedido
Nesta simulação, cada pedido contém apenas um produto. Portanto, cada pedido válido corresponde a uma linha na fato de vendas.

A partição ingest_date representa a data de ingestão, que pode ser diferente de data_pedido.

6. Regras de qualidade
Um pedido é considerado inválido quando apresenta pelo menos uma destas condições:

Regra	Motivo registrado
Quantidade menor ou igual a zero	QUANTIDADE_NAO_POSITIVA
Cliente não encontrado na dimensão	CLIENTE_INEXISTENTE
Produto não encontrado na dimensão	PRODUTO_INEXISTENTE
Cada pedido inválido é gravado uma única vez na quarentena, com uma lista contendo todos os motivos encontrados.

O pipeline também verifica duplicidades em pedido_id, cliente_id e product_id nas respectivas tabelas. Como essas duplicidades violam as premissas da base, a execução é interrompida caso sejam encontradas.

Anomalias simuladas
Pedidos	Situação	Registros
1 a 80	Válidos	80
81 a 85	Quantidade zero	5
86 a 90	Quantidade negativa	5
91 a 94	Cliente inexistente	4
95 a 98	Produto inexistente	4
99 a 100	Quantidade negativa e ambas as chaves inexistentes	2
O resultado esperado é de 80 pedidos válidos e 20 rejeitados.

A contagem por motivo é diferente da contagem de registros: existem 12 ocorrências de quantidade inválida, 6 de cliente inexistente e 6 de produto inexistente.

7. Camadas de dados
Raw
Armazena os CSVs originais, inclusive os registros com anomalias.

raw/<entidade>/ingest_date=YYYY-MM-DD/
Quarentena
Armazena os pedidos inválidos e seus motivos de rejeição.

quarantine/pedidos_rejeitados/data=YYYY-MM-DD/rejeitados.json
O formato utilizado é JSON Lines, com um objeto JSON por linha.

Silver — processed/
Contém os pedidos válidos enriquecidos com os atributos de clientes e produtos.

O pipeline calcula:

valor_total = quantidade × preco
Os cálculos monetários utilizam Decimal, evitando imprecisões de ponto flutuante.

processed/fato_vendas/ingest_date=YYYY-MM-DD/fato_vendas.parquet
Gold
Agrupa a Silver por UF e categoria dentro da partição processada e calcula:

Métrica	Definição
quantidade_pedidos	Número de pedidos válidos
unidades_vendidas	Soma das quantidades
faturamento	Soma de valor_total
ticket_medio	Faturamento dividido pela quantidade de pedidos
gold/vendas_uf_categoria/ingest_date=YYYY-MM-DD/vendas_uf_categoria.parquet
Silver e Gold são gravadas em Parquet com compressão Snappy.

8. Execução local
Pré-requisitos
Python instalado.
Dependências pandas e pyarrow.
Scripts gerar_base.py e pipeline.py na raiz do projeto.
Instalação
Crie um ambiente virtual:

python -m venv .venv
Ative no Linux/macOS:

source .venv/bin/activate
Ou no Windows PowerShell:

.venv\Scripts\Activate.ps1
Instale as dependências:

pip install pandas pyarrow
Gerar os dados
Na raiz do projeto, execute:

python gerar_base.py --data 2026-09-20 --saida dados
Configurar a data de processamento
No arquivo pipeline.py, confirme:

DATA_INGESTAO = "2026-09-20"
A data precisa corresponder à partição gerada.

Executar o pipeline
python pipeline.py
O script utiliza caminhos relativos à sua própria localização para encontrar a pasta dados.

Resultado esperado
Pedidos Raw: 100
Pedidos Silver: 80
Pedidos em quarentena: 20
Todas as verificações de conciliação devem apresentar OK.

9. Estrutura gerada
projeto/
├── gerar_base.py
├── pipeline.py
└── dados/
    ├── controle/
    │   └── ingest_date=2026-09-20/
    │       └── resultado_esperado.json
    ├── raw/
    │   ├── clientes/
    │   │   └── ingest_date=2026-09-20/clientes.csv
    │   ├── produtos/
    │   │   └── ingest_date=2026-09-20/produtos.csv
    │   └── pedidos/
    │       └── ingest_date=2026-09-20/pedidos.csv
    ├── quarantine/
    │   └── pedidos_rejeitados/
    │       └── data=2026-09-20/rejeitados.json
    ├── processed/
    │   └── fato_vendas/
    │       └── ingest_date=2026-09-20/fato_vendas.parquet
    └── gold/
        └── vendas_uf_categoria/
            └── ingest_date=2026-09-20/vendas_uf_categoria.parquet
10. Publicação no Amazon S3
O bucket utilizado no ambiente é:

s3://dados-pipeline-medallion/
As pastas devem ser enviadas diretamente para a raiz do bucket, sem incluir o diretório local dados/.

Com a AWS CLI autenticada, execute:

aws s3 sync dados/raw/ s3://dados-pipeline-medallion/raw/

aws s3 sync dados/quarantine/ s3://dados-pipeline-medallion/quarantine/

aws s3 sync dados/processed/ s3://dados-pipeline-medallion/processed/

aws s3 sync dados/gold/ s3://dados-pipeline-medallion/gold/
Também é possível enviar essas pastas pelo console do S3.

O bucket deve manter o bloqueio de acesso público habilitado. Credenciais AWS não devem ser incluídas no código ou no repositório.

O prefixo reservado para os resultados do Athena é:

s3://dados-pipeline-medallion/athena-results/
11. Configuração do Athena
No Athena:

Selecionar a região do projeto.
Configurar o local de resultados em athena-results/.
Selecionar o catálogo e o banco de dados utilizados.
Criar ou conferir as tabelas externas.
Registrar as partições.
Executar as consultas de validação.
A organização esperada das tabelas é:

Tabela de referência	Prefixo no S3	Formato	Partição
raw_clientes	raw/clientes/	CSV	ingest_date
raw_produtos	raw/produtos/	CSV	ingest_date
raw_pedidos	raw/pedidos/	CSV	ingest_date
pedidos_rejeitados	quarantine/pedidos_rejeitados/	JSON Lines	data
fato_vendas	processed/fato_vendas/	Parquet	ingest_date
vendas_uf_categoria	gold/vendas_uf_categoria/	Parquet	ingest_date
Os exemplos SQL abaixo utilizam esses nomes. Caso o catálogo use nomes como fato_vendas_parquet, ajustar as consultas.

O LOCATION de cada tabela deve apontar para o prefixo da entidade, e não diretamente para o arquivo.

Exemplo:

LOCATION 's3://dados-pipeline-medallion/raw/pedidos/'
Para tabelas criadas com as partições declaradas, é possível registrar as pastas executando individualmente:

MSCK REPAIR TABLE raw_clientes;
MSCK REPAIR TABLE raw_produtos;
MSCK REPAIR TABLE raw_pedidos;
MSCK REPAIR TABLE pedidos_rejeitados;
MSCK REPAIR TABLE fato_vendas;
MSCK REPAIR TABLE vendas_uf_categoria;
Esses comandos não criam tabelas nem corrigem um LOCATION incorreto.

12. Validação e auditoria
Verificações realizadas no Python
Antes de gravar os resultados, o pipeline verifica:

Raw = Silver + quarentena;
ausência de IDs em comum entre Silver e quarentena;
classificação de todos os pedidos da Raw;
igualdade do faturamento entre Silver e Gold;
igualdade da quantidade de pedidos entre Silver e Gold;
igualdade das unidades vendidas entre Silver e Gold.
Conciliação de registros no Athena
WITH
raw AS (
    SELECT COUNT(*) AS total
    FROM raw_pedidos
    WHERE ingest_date = '2026-09-20'
),
silver AS (
    SELECT COUNT(*) AS total
    FROM fato_vendas
    WHERE ingest_date = '2026-09-20'
),
quarentena AS (
    SELECT COUNT(*) AS total
    FROM pedidos_rejeitados
    WHERE data = '2026-09-20'
)
SELECT
    r.total AS pedidos_raw,
    s.total AS pedidos_silver,
    q.total AS pedidos_quarentena,
    r.total = s.total + q.total AS integridade_ok
FROM raw r
CROSS JOIN silver s
CROSS JOIN quarentena q;
Resultado esperado:

pedidos_raw	pedidos_silver	pedidos_quarentena	integridade_ok
100	80	20	true
Conciliação financeira
WITH
silver AS (
    SELECT SUM(valor_total) AS total
    FROM fato_vendas
    WHERE ingest_date = '2026-09-20'
),
gold AS (
    SELECT SUM(faturamento) AS total
    FROM vendas_uf_categoria
    WHERE ingest_date = '2026-09-20'
)
SELECT
    s.total AS faturamento_silver,
    g.total AS faturamento_gold,
    s.total = g.total AS integridade_ok
FROM silver s
CROSS JOIN gold g;
Auditoria do arquivo de origem
SELECT
    "$path" AS arquivo_origem,
    COUNT(*) AS quantidade_registros
FROM fato_vendas
WHERE ingest_date = '2026-09-20'
GROUP BY "$path";
Verificação de $file_size
A disponibilidade da pseudo-coluna deve ser verificada no ambiente utilizado:

SELECT DISTINCT
    "$path" AS arquivo_origem,
    "$file_size" AS tamanho_bytes
FROM fato_vendas
WHERE ingest_date = '2026-09-20';
Caso o Athena retorne COLUMN_NOT_FOUND para "$file_size", registrar a limitação e confirmar com o professor a evidência alternativa aceita. O tamanho do objeto pode ser obtido pelo S3, mas isso não substitui automaticamente a exigência da atividade.

13. Evidências da execução
Adicionar à pasta evidencias/ as capturas reais de:

estrutura dos arquivos no S3;
registros rejeitados e motivos;
dados da Silver;
indicadores da Gold;
resultado da conciliação de registros;
resultado da conciliação financeira;
consultas de metadados solicitadas.
As capturas devem mostrar a consulta executada e seu resultado. Não publicar credenciais ou outros dados sensíveis.

14. Limitações e reprocessamento
Esta implementação foi construída para a massa simulada e processa os dados em memória. Para volumes elevados, seria necessário adaptar o processamento para uma abordagem distribuída ou em lotes.

O script pressupõe o schema produzido pelo gerador. Erros estruturais, como datas malformadas ou valores não numéricos em campos numéricos, não possuem tratamento específico de quarentena.

Executar novamente a mesma data sobrescreve os arquivos de saída daquela partição. Datas diferentes são mantidas em diretórios separados.

A gravação das saídas não é transacional: uma falha durante a escrita pode deixar arquivos parcialmente atualizados entre as camadas. O reprocessamento da mesma partição permite reconstruir as saídas.

Por fim, a conciliação Python valida os dados em memória antes da gravação; as consultas no Athena verificam os arquivos efetivamente publicados no S3.
