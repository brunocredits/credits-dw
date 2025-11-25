"""
Módulo: validators.py
Descrição: Validadores de dados para garantir qualidade na camada Bronze
Versão: 1.0

Este módulo contém validadores robustos para diferentes tipos de dados.
Todos os validadores retornam tuplas (bool, str) indicando se é válido e motivo da falha.
"""

import re
from datetime import datetime
from typing import Any, Tuple, Optional, List
from decimal import Decimal, InvalidOperation


# ============================================================================
# VALIDADORES DE CAMPOS OBRIGATÓRIOS
# ============================================================================

def validar_campo_obrigatorio(valor: Any, nome_campo: str) -> Tuple[bool, Optional[str]]:
    """
    Valida se um campo obrigatório está preenchido.

    Args:
        valor: Valor a ser validado
        nome_campo: Nome do campo para mensagens de erro

    Returns:
        (True, None) se válido, (False, mensagem_erro) se inválido
    """
    if valor is None or valor == '' or (isinstance(valor, str) and valor.strip() == ''):
        return False, f"Campo obrigatório '{nome_campo}' está vazio ou nulo"
    return True, None


# ============================================================================
# VALIDADORES DE FORMATO
# ============================================================================

def validar_data(valor: Any, formato: str = '%Y-%m-%d') -> Tuple[bool, Optional[str]]:
    """
    Valida se um valor é uma data válida.

    Args:
        valor: Valor a ser validado
        formato: Formato esperado da data (padrão: YYYY-MM-DD)

    Returns:
        (True, None) se válido, (False, mensagem_erro) se inválido
    """
    if valor is None or valor == '':
        return False, "Data está vazia ou nula"

    try:
        if isinstance(valor, str):
            datetime.strptime(valor, formato)
        elif isinstance(valor, datetime):
            pass  # Já é datetime, válido
        else:
            return False, f"Tipo de dado inválido para data: {type(valor).__name__}"
        return True, None
    except ValueError as e:
        return False, f"Data inválida (esperado formato {formato}): {valor}"


def validar_email(valor: Any) -> Tuple[bool, Optional[str]]:
    """
    Valida se um valor é um email válido.

    Args:
        valor: Valor a ser validado

    Returns:
        (True, None) se válido, (False, mensagem_erro) se inválido
    """
    if valor is None or valor == '':
        return False, "Email está vazio ou nulo"

    if not isinstance(valor, str):
        return False, f"Email deve ser string, recebido: {type(valor).__name__}"

    # Regex básico para validação de email
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(pattern, valor.strip()):
        return False, f"Formato de email inválido: {valor}"

    return True, None


def validar_cnpj_cpf(valor: Any) -> Tuple[bool, Optional[str]]:
    """
    Valida se um valor é CNPJ ou CPF válido (formato e dígitos verificadores).

    Args:
        valor: Valor a ser validado

    Returns:
        (True, None) se válido, (False, mensagem_erro) se inválido
    """
    if valor is None or valor == '':
        return False, "CNPJ/CPF está vazio ou nulo"

    if not isinstance(valor, str):
        valor = str(valor)

    # Remove caracteres não numéricos
    numeros = re.sub(r'[^0-9]', '', valor)

    if len(numeros) == 11:
        # Validação de CPF
        return _validar_cpf(numeros)
    elif len(numeros) == 14:
        # Validação de CNPJ
        return _validar_cnpj(numeros)
    else:
        return False, f"CNPJ/CPF deve ter 11 (CPF) ou 14 (CNPJ) dígitos, recebido {len(numeros)}: {valor}"


def _validar_cpf(cpf: str) -> Tuple[bool, Optional[str]]:
    """Validação de CPF com dígitos verificadores"""
    # Verificar CPFs inválidos conhecidos (todos dígitos iguais)
    if cpf == cpf[0] * 11:
        return False, f"CPF inválido (dígitos repetidos): {cpf}"

    # Validar primeiro dígito verificador
    soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
    digito1 = 11 - (soma % 11)
    digito1 = 0 if digito1 > 9 else digito1

    if int(cpf[9]) != digito1:
        return False, f"CPF inválido (dígito verificador incorreto): {cpf}"

    # Validar segundo dígito verificador
    soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
    digito2 = 11 - (soma % 11)
    digito2 = 0 if digito2 > 9 else digito2

    if int(cpf[10]) != digito2:
        return False, f"CPF inválido (dígito verificador incorreto): {cpf}"

    return True, None


def _validar_cnpj(cnpj: str) -> Tuple[bool, Optional[str]]:
    """Validação de CNPJ com dígitos verificadores"""
    # Verificar CNPJs inválidos conhecidos (todos dígitos iguais)
    if cnpj == cnpj[0] * 14:
        return False, f"CNPJ inválido (dígitos repetidos): {cnpj}"

    # Validar primeiro dígito verificador
    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma = sum(int(cnpj[i]) * pesos1[i] for i in range(12))
    digito1 = 11 - (soma % 11)
    digito1 = 0 if digito1 > 9 else digito1

    if int(cnpj[12]) != digito1:
        return False, f"CNPJ inválido (dígito verificador incorreto): {cnpj}"

    # Validar segundo dígito verificador
    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma = sum(int(cnpj[i]) * pesos2[i] for i in range(13))
    digito2 = 11 - (soma % 11)
    digito2 = 0 if digito2 > 9 else digito2

    if int(cnpj[13]) != digito2:
        return False, f"CNPJ inválido (dígito verificador incorreto): {cnpj}"

    return True, None


# ============================================================================
# VALIDADORES NUMÉRICOS
# ============================================================================

def validar_numero(valor: Any, tipo: str = 'decimal') -> Tuple[bool, Optional[str]]:
    """
    Valida se um valor é um número válido.

    Args:
        valor: Valor a ser validado
        tipo: Tipo de número esperado ('int', 'float', 'decimal')

    Returns:
        (True, None) se válido, (False, mensagem_erro) se inválido
    """
    if valor is None or valor == '':
        return False, "Número está vazio ou nulo"

    try:
        if tipo == 'int':
            int(valor)
        elif tipo == 'float':
            float(valor)
        elif tipo == 'decimal':
            Decimal(str(valor))
        else:
            return False, f"Tipo numérico desconhecido: {tipo}"
        return True, None
    except (ValueError, InvalidOperation) as e:
        return False, f"Valor não é um {tipo} válido: {valor}"


def validar_numero_positivo(valor: Any) -> Tuple[bool, Optional[str]]:
    """
    Valida se um número é positivo (> 0).

    Args:
        valor: Valor a ser validado

    Returns:
        (True, None) se válido, (False, mensagem_erro) se inválido
    """
    valido, msg = validar_numero(valor)
    if not valido:
        return False, msg

    try:
        num = Decimal(str(valor))
        if num <= 0:
            return False, f"Número deve ser positivo (> 0), recebido: {valor}"
        return True, None
    except (ValueError, InvalidOperation):
        return False, f"Erro ao validar número positivo: {valor}"


def validar_numero_nao_negativo(valor: Any) -> Tuple[bool, Optional[str]]:
    """
    Valida se um número é não-negativo (>= 0).

    Args:
        valor: Valor a ser validado

    Returns:
        (True, None) se válido, (False, mensagem_erro) se inválido
    """
    valido, msg = validar_numero(valor)
    if not valido:
        return False, msg

    try:
        num = Decimal(str(valor))
        if num < 0:
            return False, f"Número deve ser não-negativo (>= 0), recebido: {valor}"
        return True, None
    except (ValueError, InvalidOperation):
        return False, f"Erro ao validar número não-negativo: {valor}"


# ============================================================================
# VALIDADORES DE DOMÍNIO
# ============================================================================

def validar_valor_dominio(valor: Any, valores_permitidos: List[str],
                          case_sensitive: bool = False) -> Tuple[bool, Optional[str]]:
    """
    Valida se um valor está dentro de um domínio específico (lista de valores permitidos).

    Args:
        valor: Valor a ser validado
        valores_permitidos: Lista de valores permitidos
        case_sensitive: Se a comparação deve ser case-sensitive

    Returns:
        (True, None) se válido, (False, mensagem_erro) se inválido
    """
    if valor is None or valor == '':
        return False, "Valor está vazio ou nulo"

    valor_str = str(valor)

    if not case_sensitive:
        valor_str = valor_str.upper()
        valores_permitidos = [v.upper() for v in valores_permitidos]

    if valor_str not in valores_permitidos:
        return False, f"Valor '{valor}' não está no domínio permitido: {valores_permitidos}"

    return True, None


def validar_tamanho_string(valor: Any, min_len: Optional[int] = None,
                           max_len: Optional[int] = None) -> Tuple[bool, Optional[str]]:
    """
    Valida se o tamanho de uma string está dentro dos limites.

    Args:
        valor: Valor a ser validado
        min_len: Tamanho mínimo (opcional)
        max_len: Tamanho máximo (opcional)

    Returns:
        (True, None) se válido, (False, mensagem_erro) se inválido
    """
    if valor is None or valor == '':
        return False, "String está vazia ou nula"

    valor_str = str(valor)
    tamanho = len(valor_str)

    if min_len is not None and tamanho < min_len:
        return False, f"String muito curta (mínimo {min_len} caracteres): '{valor}' ({tamanho} chars)"

    if max_len is not None and tamanho > max_len:
        return False, f"String muito longa (máximo {max_len} caracteres): '{valor}' ({tamanho} chars)"

    return True, None


# ============================================================================
# VALIDADOR COMPOSTO
# ============================================================================

def validar_campo(valor: Any, nome_campo: str, regras: dict) -> Tuple[bool, Optional[str]]:
    """
    Validador composto que aplica múltiplas regras a um campo.

    Args:
        valor: Valor a ser validado
        nome_campo: Nome do campo para mensagens de erro
        regras: Dicionário com regras de validação
            {
                'obrigatorio': True/False,
                'tipo': 'string'/'int'/'float'/'decimal'/'data'/'email'/'cnpj_cpf',
                'minimo': valor_minimo (para números),
                'maximo': valor_maximo (para números),
                'min_len': tamanho_minimo (para strings),
                'max_len': tamanho_maximo (para strings),
                'dominio': lista_valores_permitidos,
                'formato_data': '%Y-%m-%d' (para datas)
            }

    Returns:
        (True, None) se válido, (False, mensagem_erro) se inválido

    Example:
        >>> validar_campo('123.456.789-00', 'cpf', {'obrigatorio': True, 'tipo': 'cnpj_cpf'})
        (True, None)
    """
    # Validação de campo obrigatório
    if regras.get('obrigatorio', False):
        valido, msg = validar_campo_obrigatorio(valor, nome_campo)
        if not valido:
            return False, msg
    else:
        # Se não é obrigatório e está vazio, considera válido
        if valor is None or valor == '' or (isinstance(valor, str) and valor.strip() == ''):
            return True, None

    # Validação por tipo
    tipo = regras.get('tipo', 'string')

    if tipo == 'email':
        return validar_email(valor)

    elif tipo == 'cnpj_cpf':
        return validar_cnpj_cpf(valor)

    elif tipo == 'data':
        formato = regras.get('formato_data', '%Y-%m-%d')
        return validar_data(valor, formato)

    elif tipo in ['int', 'float', 'decimal']:
        valido, msg = validar_numero(valor, tipo)
        if not valido:
            return False, msg

        # Validação de range numérico
        num = Decimal(str(valor))

        if 'minimo' in regras and num < Decimal(str(regras['minimo'])):
            return False, f"Valor {valor} menor que mínimo permitido ({regras['minimo']})"

        if 'maximo' in regras and num > Decimal(str(regras['maximo'])):
            return False, f"Valor {valor} maior que máximo permitido ({regras['maximo']})"

        if regras.get('positivo', False):
            valido, msg = validar_numero_positivo(valor)
            if not valido:
                return False, msg

        if regras.get('nao_negativo', False):
            valido, msg = validar_numero_nao_negativo(valor)
            if not valido:
                return False, msg

    elif tipo == 'string':
        # Validação de tamanho de string
        if 'min_len' in regras or 'max_len' in regras:
            valido, msg = validar_tamanho_string(
                valor,
                regras.get('min_len'),
                regras.get('max_len')
            )
            if not valido:
                return False, msg

    # Validação de domínio
    if 'dominio' in regras:
        case_sensitive = regras.get('case_sensitive', False)
        return validar_valor_dominio(valor, regras['dominio'], case_sensitive)

    return True, None
