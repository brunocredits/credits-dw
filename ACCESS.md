# Gerenciamento de Acessos e Usuários

Este documento detalha a estrutura de segurança e acesso ao banco de dados `creditsdw`.

## 👥 Roles e Grupos

### `dw_developer`
Grupo destinado à equipe técnica e analistas de dados.
*   **Permissões:** Leitura e Escrita (`SELECT`, `INSERT`, `UPDATE`, `DELETE`) nos schemas `bronze` e `auditoria`.
*   **Membros:**
    *   `bruno_cavalcante`
    *   `maria_rodrigues`
    *   `joao_viveiros`
    *   `crislaine_cardoso`

## 🔑 Credenciais Padrão

Para o primeiro acesso, foi definida uma senha padrão para todos os usuários acima.
> **Senha Padrão:** `Credits@2025`

⚠️ **Importante:** Cada usuário deve alterar sua senha no primeiro login utilizando o comando:
```sql
ALTER USER meu_usuario WITH PASSWORD 'MinhaNovaSenhaForte!';
```

## 🛠️ Como Conectar

Utilize as credenciais abaixo no seu cliente SQL (DBeaver, pgAdmin, Datagrip):

*   **Host:** `creditsbrasil.postgres.database.azure.com`
*   **Porta:** `5432`
*   **Database:** `creditsdw`
*   **Username:** *<seu_usuario_acima>*
*   **Password:** *<senha_padrao>* (ou a nova senha definida)
*   **SSL Mode:** Require (recomendado para Azure)

## 🔄 Manutenção de Usuários

Para adicionar novos usuários ou resetar permissões, utilize o script `python/scripts/setup_access.py`.

```bash
# Executar via Docker
./run_pipeline.sh python python/scripts/setup_access.py
```
