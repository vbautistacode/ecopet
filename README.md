# Streamdash

Aplicação de BI em Streamlit para integração com múltiplos ERPs.

## Estrutura
- `app/` → aplicação principal (dashboards, inputs, autenticação)
- `etl/` → pipelines de ETL (load, transform, write)
- `db/` → conexão e modelos de banco
- `tests/` → testes unitários

## Como rodar
```bash
pip install -r requirements.txt
streamlit run app/main.py

### Resumo rápido
**Streamlit** como front‑end e **Supabase** como banco (Postgres + Storage + Auth) podem atender perfeitamente ao seu caso: uploads de planilhas, ETL, DRE e dashboards. Porém isso só é seguro e robusto se você projetar corretamente autenticação, execução do ETL e controle de acesso. Abaixo explico riscos, arquitetura recomendada e passos práticos para operar com segurança e sem perder histórico.

---

### Arquitetura recomendada (visão prática)
**Componentes**
- **Streamlit app** (frontend) — interface para upload e visualização.  
- **Supabase Storage** — armazena os arquivos brutos (CSV/XLSX).  
- **Supabase Postgres** — staging + tabelas fato/dimensão.  
- **Backend de processamento** — função serverless (Supabase Edge Function) ou worker (Docker/Celery) que executa o ETL.  
- **Auth e secrets** — variáveis de ambiente no servidor; **nunca** expor `service_role` no frontend.  
- **Observability** — logs, métricas, backups.

**Fluxo**
1. Usuário faz upload no Streamlit → arquivo vai para **Storage**.  
2. Streamlit registra metadados em `uploads` (DB) e aciona job (HTTP call / queue).  
3. **Backend** pega o arquivo do Storage, valida, carrega em staging e faz upsert nas tabelas fato.  
4. Dashboards leem as tabelas consolidadas.

---

### Segurança e controle de acesso (crucial)
**Faça**:
- **Backend com service_role**: apenas o backend/worker usa a chave `service_role` para ETL.  
- **Frontend usa anon/authenticated**: operações do usuário (upload) usam chaves públicas limitadas.  
- **Signed URLs** para downloads/uploads do Storage.  
- **RBAC**: crie roles no Postgres e policies RLS para proteger dados sensíveis.  
- **Audit trail**: registre `upload_id`, `uploader`, `import_batch_id` e `imported_at`.  
- **Rotação de credenciais** e armazenamento seguro (secrets manager, variáveis de ambiente no servidor).  
- **HTTPS** e autenticação forte (2FA para contas administrativas).

---

### Sobre compartilhar um único usuário/senha
- **Risco**: perda de rastreabilidade, dificuldade para revogar acesso, maior superfície de ataque.  
- **Se for inevitável (temporário)**:
  - Use conta com **privilégios mínimos**; não use `service_role`.  
  - Restrinja acesso por **IP** (se possível) e registre todas as ações.  
  - Troque a senha periodicamente e mantenha logs de auditoria.  
- **Melhor alternativa**: crie contas separadas (você + cliente) com permissões adequadas e use autenticação via Supabase Auth.

---

### Execução do ETL e escalabilidade
- **Não execute ETL no Streamlit**: Streamlit é para UI; ETL deve rodar em backend (Edge Function, worker, cron).  
- **Jobs assíncronos**: ao subir arquivo, enfileire um job (Redis, Supabase Realtime, ou HTTP webhook) e processe em background.  
- **Chunking e COPY**: para arquivos grandes, use `COPY` para staging e depois `INSERT ... ON CONFLICT` para upsert.  
- **Particionamento**: particione `fact_sales` por mês se volume for alto.  
- **Monitoramento**: métricas de tempo de processamento, filas e falhas.

---

### Backup, recuperação e integridade do backlog
- **Backups automáticos**: habilite snapshots regulares do Postgres (Supabase oferece backups).  
- **Import batch tracking**: cada upload gera `import_batch_id` e não sobrescreve histórico; use `imported_at` e `source_file`.  
- **Validações**: rejeite/registre linhas inválidas em `upload_errors` para revisão manual.  
- **Testes de reconciliação**: compare somas do `fluxo_de_caixa_diario` com `fact_payables` periodicamente.

---

### Checklist prático para colocar em produção hoje
- [ ] Armazenar arquivos no **Supabase Storage** e registrar `uploads`.  
- [ ] Implementar **Edge Function** ou worker que: baixa arquivo, valida, carrega em staging e faz upsert.  
- [ ] Garantir **service_role** só no backend; frontend usa `anon`/`authenticated`.  
- [ ] Criar **UNIQUE constraints** e `ON CONFLICT` upsert para idempotência.  
- [ ] Adicionar `import_batch_id` em staging e fatos.  
- [ ] Habilitar backups automáticos e logs de auditoria.  
- [ ] Configurar monitoramento e alertas (falha de ETL, fila parada, uso de disco).