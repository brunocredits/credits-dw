"""
Este módulo, `validators`, fornece um conjunto de funções para validar
diferentes tipos de dados, garantindo a qualidade e a integridade dos dados
que entram na camada Bronze do data warehouse.

Os validadores cobrem uma variedade de cenários, desde checagens simples, como
campos obrigatórios, até validações complexas, como formatos de data, CPF/CNPJ
e regras de domínio. Cada função de validação retorna uma tupla `(bool, str)`,
indicando o sucesso da validação e uma mensagem de erro, se aplicável.
"""

import re
from datetime import datetime
from typing import Any, Tuple, Optional, List
from decimal import Decimal, InvalidOperation

# --- VALIDADORES DE FORMATO E CONTEÚDO ---

def validar_campo_obrigatorio(valor: Any, nome_campo: str) -> Tuple[bool, Optional[str]]:
    """
    Verifica se um campo essencial está preenchido.

    Args:
        valor: O valor do campo a ser verificado.
        nome_campo: O nome do campo, usado para gerar a mensagem de erro.

    Returns:
        Uma tupla (True, None) se o campo for válido, ou (False, mensagem_de_erro)
        se for nulo ou vazio.
    """
    if valor is None or (isinstance(valor, str) and not valor.strip()):
        return False, f"O campo obrigatório '{nome_campo}' está vazio."
    return True, None

def validar_data(valor: Any, formato: str = '%Y-%m-%d') -> Tuple[bool, Optional[str]]:
    """
    Valida se um valor representa uma data em um formato específico.

    Args:
        valor: O valor a ser validado.
        formato: O formato de data esperado (ex: '%d/%m/%Y').

    Returns:
        (True, None) se a data for válida, (False, mensagem_de_erro) caso contrário.
    """
    if not valor:
        return False, "O valor da data não pode ser vazio."
    try:
        if isinstance(valor, str):
            datetime.strptime(valor, formato)
        elif not isinstance(valor, datetime):
            return False, f"Tipo de dado inválido para data: {type(valor).__name__}."
        return True, None
    except ValueError:
        return False, f"A data '{valor}' não corresponde ao formato esperado '{formato}'."

def validar_cnpj_cpf(valor: Any) -> Tuple[bool, Optional[str]]:
    """
    Valida se um valor é um CPF ou CNPJ com dígitos verificadores corretos.
    Remove caracteres não numéricos antes da validação.
    """
    if not valor:
        return False, "CPF/CNPJ não pode ser vazio."
    
    numeros = re.sub(r'[^0-9]', '', str(valor))

    if len(numeros) == 11:
        return _validar_cpf(numeros)
    elif len(numeros) == 14:
        return _validar_cnpj(numeros)
    else:
        return False, f"CPF/CNPJ com tamanho inválido ({len(numeros)} dígitos)."

def _validar_cpf(cpf: str) -> Tuple[bool, Optional[str]]:
    """Lógica de validação do dígito verificador do CPF."""
    if cpf == cpf[0] * 11:
        return False, f"CPF inválido (todos os dígitos são iguais): {cpf}"
    
    # Cálculo do primeiro dígito
    soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
    digito1 = (soma * 10) % 11
    if digito1 == 10: digito1 = 0
    if digito1 != int(cpf[9]):
        return False, f"CPF com primeiro dígito verificador inválido: {cpf}"
        
    # Cálculo do segundo dígito
    soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
    digito2 = (soma * 10) % 11
    if digito2 == 10: digito2 = 0
    if digito2 != int(cpf[10]):
        return False, f"CPF com segundo dígito verificador inválido: {cpf}"
        
    return True, None

def _validar_cnpj(cnpj: str) -> Tuple[bool, Optional[str]]:
    """Lógica de validação do dígito verificador do CNPJ."""
    if cnpj == cnpj[0] * 14:
        return False, f"CNPJ inválido (todos os dígitos são iguais): {cnpj}"

    # Cálculo do primeiro dígito
    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma = sum(int(cnpj[i]) * pesos1[i] for i in range(12))
    digito1 = 11 - (soma % 11)
    if digito1 >= 10: digito1 = 0
    if digito1 != int(cnpj[12]):
        return False, f"CNPJ com primeiro dígito verificador inválido: {cnpj}"

    # Cálculo do segundo dígito
    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma = sum(int(cnpj[i]) * pesos2[i] for i in range(13))
    digito2 = 11 - (soma % 11)
    if digito2 >= 10: digito2 = 0
    if digito2 != int(cnpj[13]):
        return False, f"CNPJ com segundo dígito verificador inválido: {cnpj}"
        
    return True, None

# --- VALIDADORES NUMÉRICOS ---

def validar_numero(valor: Any, tipo: str = 'decimal') -> Tuple[bool, Optional[str]]:
    """
    Valida se um valor pode ser convertido para um tipo numérico específico.
    """
    if valor is None or (isinstance(valor, str) and not valor.strip()):
        return False, "O valor numérico não pode ser vazio."
    try:
        if tipo == 'int': int(valor)
        elif tipo == 'float': float(valor)
        elif tipo == 'decimal': Decimal(str(valor))
        else: return False, f"Tipo numérico desconhecido: {tipo}"
        return True, None
    except (ValueError, InvalidOperation):
        return False, f"O valor '{valor}' não é um '{tipo}' válido."

def validar_numero_nao_negativo(valor: Any) -> Tuple[bool, Optional[str]]:
    """
    Valida se um valor é um número maior ou igual a zero.
    """
    valido, msg = validar_numero(valor)
    if not valido:
        return False, msg
    if Decimal(str(valor)) < 0:
        return False, f"O número deve ser não-negativo (>= 0), mas foi '{valor}'."
    return True, None

# --- VALIDADORES DE DOMÍNIO E REGRAS DE NEGÓCIO ---

def validar_valor_dominio(valor: Any, valores_permitidos: List[str], case_sensitive: bool = False) -> Tuple[bool, Optional[str]]:
    """
    Verifica se um valor pertence a uma lista predefinida de valores (domínio).
    """
    if valor is None:
        return False, "O valor não pode ser nulo para validação de domínio."
    
    valor_str = str(valor)
    if not case_sensitive:
        valor_str = valor_str.upper()
        valores_permitidos = [v.upper() for v in valores_permitidos]

    if valor_str not in valores_permitidos:
        return False, f"O valor '{valor}' não pertence ao domínio permitido: {valores_permitidos}."
    return True, None

def validar_tamanho_string(valor: Any, min_len: Optional[int] = None, max_len: Optional[int] = None) -> Tuple[bool, Optional[str]]:
    """
    Valida se o comprimento de uma string está dentro de um intervalo definido.
    """
    if valor is None:
        return False, "A string não pode ser nula para validação de tamanho."
        
    tamanho = len(str(valor))
    if min_len is not None and tamanho < min_len:
        return False, f"A string é muito curta (mínimo de {min_len} caracteres)."
    if max_len is not None and tamanho > max_len:
        return False, f"A string é muito longa (máximo de {max_len} caracteres)."
    return True, None
