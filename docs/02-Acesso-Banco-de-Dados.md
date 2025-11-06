# 02 - Acesso ao Banco de Dados

Este documento detalha como se conectar ao banco de dados PostgreSQL do Data Warehouse, incluindo a configuração de firewall e o gerenciamento de credenciais.

---

## 1. Detalhes da Conexão

Use as seguintes informações em qualquer cliente SQL (DBeaver, pgAdmin, etc.) para se conectar ao banco de dados.

*   **Host:** `creditsdw.postgres.database.azure.com`
*   **Porta:** `5432`
*   **Banco de Dados (Database):** `creditsdw`
*   **Método de Autenticação:** Senha Padrão (ou "Database Native")
*   **Modo SSL:** `require` (obrigatório para garantir uma conexão segura)

---

## 2. Configuração de Firewall (Azure)

O banco de dados no Azure é protegido por um firewall. Para que a conexão da sua máquina seja bem-sucedida, seu endereço IP público precisa ser liberado.

1.  **Acesse o Portal do Azure** e faça login.
2.  Navegue até o servidor PostgreSQL `creditsdw`.
3.  No menu lateral, vá para a seção **"Networking"**.
4.  Clique em **"Add current client IP address"** para adicionar seu IP atual à lista de permissões.
5.  Clique em **"Save"** para aplicar a nova regra.

**Importante:** Se você trocar de rede (ex: ir do escritório para casa), seu IP público mudará, e você precisará repetir este processo.

---

## 3. Credenciais de Acesso

Utilizamos usuários "nativos" do PostgreSQL para garantir a compatibilidade com diversas ferramentas. Cada membro da equipe tem um usuário e uma senha.

**Senha Padrão para o Primeiro Acesso:** `Credits@DW!2025`

| Usuário (Username)       | Role Atribuída   | Nível de Acesso                         |
| :----------------------- | :--------------- | :-------------------------------------- |
| `bruno_pires_pg`         | `dw_admin`       | Acesso total de administrador ao DW.    |
| `joao_viveiros_pg`       | `dw_developer`   | Acesso de leitura e escrita nos dados.  |
| `maria_rodrigues_pg`     | `dw_developer`   | Acesso de leitura e escrita nos dados.  |

---

## 4. Como Alterar Sua Senha

É **altamente recomendado** que você altere a senha padrão no seu primeiro acesso.

1.  Conecte-se ao banco de dados com seu usuário e a senha padrão.
2.  Abra uma nova janela de consulta SQL.
3.  Execute o comando abaixo, substituindo `<seu_username>` e a nova senha.

    ```sql
    ALTER USER <seu_username> WITH PASSWORD 'SuaNovaSenhaSuperForte123!';
    ```

    **Exemplo:**
    ```sql
    ALTER USER joao_viveiros_pg WITH PASSWORD 'SuaNovaSenhaSuperForte123!';
    ```

---

Com o acesso ao banco de dados configurado, o próximo passo é aprender a executar os processos de ETL, o que está descrito no arquivo `03-Executando-ETL.md`.
