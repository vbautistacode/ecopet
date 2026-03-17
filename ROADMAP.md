# Roadmap: Normalização de dados para dashboards

Objetivo
- Implementar uma camada de ingestão/normalização canônica para todas as fontes de dados usadas pelos dashboards:
  financeiro, dre, marketing, clientes, setor, estratégicos, etc.

Escopo inicial
- Definir esquema canônico por domínio (ex.: cashflow: date, cash_in, cash_out, saldo, caixa).
- Implementar funções `normalize_<domain>(df)` que:
  - mapeiam sinônimos de colunas para nomes canônicos;
  - limpam e convertem tipos (números, datas);
  - geram colunas derivadas (saldo, date_id);
  - agregam quando necessário (por mês, por produto).
- Integrar normalizadores em `fetch_all_tables()` e nos pontos que chamam `show_*` dos dashboards.
- Adicionar testes unitários e exemplos de entrada/saída.
- Cachear resultados com `@st.cache_data` para performance.

Prioridade e entregáveis
1. Implementar `normalize_cashflow` e integrar com `show_finance`.
2. Repetir padrão para DRE e Vendas.
3. Cobrir Marketing, Clientes, Setor e Estratégicos.
4. Criar migrations/checagens para garantir colunas mínimas no DB (upload_errors, staging).