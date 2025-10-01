# 📊 Benchmark de Engines de Processamento de Dados

## 🎯 Sobre o Projeto

Este projeto implementa um sistema abrangente de benchmark para comparar a performance de diferentes engines de processamento de dados em Python: **DuckDB**, **Pandas** e **Polars**. O benchmark analisa operações comuns de manipulação de dados utilizando os microdados do ENEM 2023.

## 🏗️ Arquitetura do Sistema

### Engines Testadas
- **🦆 DuckDB**: Engine SQL para analytics embarcada
- **🐼 Pandas**: Biblioteca tradicional para análise de dados em Python  
- **⚡ Polars**: Engine moderna de DataFrames com foco em performance

### Operações Benchmarkadas
1. **Leitura**: CSV e Parquet
2. **Filtros**: Condições WHERE em colunas numéricas
3. **Joins**: Operações de junção entre datasets
4. **Agregações**: Agrupamentos e cálculos estatísticos
5. **Escrita**: Exportação para CSV e Parquet

### Cenários de Teste
- **🟢 Pequeno**: Dataset reduzido para testes rápidos
- **🟡 Médio**: Dataset intermediário para simulação realística  
- **🔴 Grande**: Dataset completo para teste de escalabilidade

## 📁 Estrutura do Projeto

```
benchmark/
├── main.py                           # Script principal de benchmark
├── run_all_benchmarks.py            # Execução automatizada de todos os cenários
├── requirements.txt                  # Dependências do projeto
├── pyproject.toml                   # Configurações do Ruff (linter/formatter)
├── README.md                        # Este arquivo
├── CONSOLIDACAO_README.md           # Documentação do sistema de consolidação
├── README_PARAMETRIZACAO.md         # Documentação da parametrização
├── RUFF_USAGE.md                    # Guia de uso do Ruff
├── microdados_enem_2023/            # Dataset do ENEM 2023
│   ├── DADOS/
│   │   ├── MICRODADOS_ENEM_2023.csv           # Dataset grande (latin-1)
│   │   ├── MICRODADOS_ENEM_2023_medio.csv     # Dataset médio (utf-8)
│   │   ├── MICRODADOS_ENEM_2023_pequeno.csv   # Dataset pequeno (utf-8)
│   │   ├── *.parquet                          # Versões em Parquet
│   │   └── ITENS_PROVA_2023.csv              # Dados auxiliares para joins
│   ├── DICIONÁRIO/                           # Documentação dos dados
│   ├── PROVAS E GABARITOS/                   # Material adicional
│   └── ...
├── benchmark_resultados_*.csv       # Resultados individuais por cenário
├── resultados_geral.csv            # Resultados consolidados
└── output.*                        # Arquivos temporários gerados pelos testes
```

## 🚀 Como Usar

### Pré-requisitos
```bash
# Python 3.8+
python --version

# Instalar dependências
pip install -r requirements.txt
```

### Execução Rápida
```bash
# Executar um cenário específico
python main.py pequeno   # ~2-5 minutos
python main.py medio     # ~5-15 minutos  
python main.py grande    # ~15-60 minutos

# Executar todos os cenários automaticamente
python run_all_benchmarks.py
```

### Parâmetros de Configuração

No arquivo `main.py`, você pode ajustar:
```python
TEST_REPEATS = 10  # Número de repetições para cada teste (padrão: 10)
```

## 📊 Resultados e Análise

### Arquivos Gerados

1. **Resultados Individuais**:
   - `benchmark_resultados_pequeno.csv`
   - `benchmark_resultados_medio.csv`
   - `benchmark_resultados_grande.csv`

2. **Resultado Consolidado**:
   - `resultados_geral.csv` - Todos os resultados em um arquivo único

### Métricas Coletadas

- **⏱️ Tempo de Execução**: Tempo em segundos para cada operação
- **🧠 Uso de Memória**: Pico de memória RAM utilizada em MB
- **📈 Estatísticas**: Médias, desvios padrão e análises comparativas

### Estrutura dos Resultados
```csv
engine,operation,scenario,time_seconds,memory_mb
duckdb,read_csv,pequeno,1.234,45.2
pandas,read_csv,pequeno,2.456,78.9
polars,read_csv,pequeno,0.987,32.1
...
```

## 🔧 Funcionalidades Avançadas

### 1. Sistema de Encoding Inteligente
- **Pequeno/Médio**: UTF-8 encoding
- **Grande**: Latin-1 encoding (para compatibilidade com dados oficiais)

### 2. Consolidação Automática
- Combina resultados de múltiplas execuções
- Gera estatísticas resumidas automaticamente
- Detecta e processa arquivos existentes

### 3. Monitoramento de Recursos
- Limpeza automática de garbage collector
- Medição precisa do uso de memória
- Estabilização entre medições

### 4. Tratamento de Erros
- Captura e registra erros por engine/operação
- Continua execução mesmo com falhas individuais
- Logs detalhados para debugging

## 📈 Casos de Uso

### Para Pesquisadores
- Comparar performance de diferentes engines
- Analisar escalabilidade com tamanhos de dados crescentes
- Identificar gargalos em operações específicas

### Para Desenvolvedores
- Escolher a melhor engine para diferentes cenários
- Otimizar pipelines de processamento de dados
- Validar performance antes de mudanças de arquitetura

### Para Analistas de Dados
- Entender trade-offs entre facilidade de uso e performance
- Planejar recursos computacionais para projetos
- Otimizar workflows de análise

## 🛠️ Tecnologias Utilizadas

### Core Dependencies
- **DuckDB** `1.3.1` - Engine SQL embarcada
- **Pandas** `2.3.0` - Manipulação de dados tradicional
- **Polars** `1.30.0` - Engine moderna de DataFrames
- **PyArrow** `20.0.0` - Backend para formatos colunares

### Utilities
- **psutil** `7.0.0` - Monitoramento de sistema
- **memory-profiler** `0.61.0` - Profiling de memória
- **Ruff** `≥0.1.0` - Linter e formatter Python

## 📋 Metodologia de Benchmark

### Processo de Medição
1. **Preparação**: Limpeza de garbage collector (3x)
2. **Estabilização**: Pausa para estabilizar memória
3. **Medição Inicial**: Múltiplas leituras de memória base
4. **Execução**: Operação sendo benchmarkada
5. **Medição Final**: Captura do pico de memória
6. **Limpeza**: Garbage collection e liberação de recursos

### Controles de Qualidade
- Múltiplas repetições por teste (padrão: 10)
- Medições de memória com múltiplas amostras
- Uso do menor valor de memória inicial para precisão
- Força coleta de garbage entre medições

## 🤝 Contribuindo

### Desenvolvimento Local
```bash
# Configurar ambiente
python -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
venv\Scripts\activate     # Windows

pip install -r requirements.txt

# Executar linter/formatter
ruff check .              # Verificar código
ruff format .             # Formatar código
```

### Adicionando Novas Engines
1. Implementar funções seguindo o padrão: `engine_operation(path, encoding="utf-8")`
2. Adicionar à lista de engines na seção de execução
3. Testar com diferentes cenários

### Adicionando Novas Operações
1. Implementar para todas as engines existentes
2. Adicionar ao mapeamento de operações
3. Documentar a nova funcionalidade

## 📜 Licença

Este projeto é desenvolvido para fins acadêmicos e de pesquisa. Os dados do ENEM são de domínio público, disponibilizados pelo INEP.

## 📞 Suporte

Para dúvidas, sugestões ou problemas:
- Abra uma issue no repositório
- Consulte a documentação adicional em `/docs/`
- Verifique os logs de execução para debugging

---

**💡 Dica**: Execute primeiro o cenário "pequeno" para validar a configuração antes de rodar benchmarks completos nos datasets maiores.

**⚠️ Atenção**: O cenário "grande" pode consumir recursos significativos de CPU e memória. Monitore o sistema durante a execução.