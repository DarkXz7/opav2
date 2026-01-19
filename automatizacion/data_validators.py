#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Validadores de Datos para el Sistema de Carga
Contiene validadores especializados para diferentes tipos de datos y reglas de negocio
"""
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime
import re
import json

class DataValidators:
    """
    Conjunto de validadores para diferentes tipos de datos y reglas de negocio
    """
    
    @staticmethod
    def create_user_validation_rules() -> Dict[str, Any]:
        """
        Reglas de validación para tabla de usuarios
        """
        return {
            'required_fields': ['NombreUsuario', 'Email', 'NombreCompleto'],
            'unique_field': 'NombreUsuario',
            'email_field': 'Email',
            'max_length': {
                'NombreUsuario': 100,
                'Email': 255,
                'NombreCompleto': 200
            },
            'format_validations': {
                'Email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            }
        }
    
    @staticmethod
    def create_transaction_validation_rules() -> Dict[str, Any]:
        """
        Reglas de validación para tablas de transacciones
        """
        return {
            'required_fields': ['TransactionID', 'Amount', 'Date', 'UserID'],
            'unique_field': 'TransactionID',
            'numeric_fields': ['Amount', 'UserID'],
            'date_fields': ['Date'],
            'min_values': {
                'Amount': 0.01
            },
            'max_values': {
                'Amount': 999999.99
            }
        }
    
    @staticmethod
    def create_inventory_validation_rules() -> Dict[str, Any]:
        """
        Reglas de validación para tablas de inventario
        """
        return {
            'required_fields': ['ProductID', 'ProductName', 'Quantity', 'Price'],
            'unique_field': 'ProductID',
            'numeric_fields': ['Quantity', 'Price'],
            'min_values': {
                'Quantity': 0,
                'Price': 0.01
            },
            'max_length': {
                'ProductName': 255,
                'Description': 1000
            }
        }
    
    @staticmethod
    def validate_email_format(email: str) -> bool:
        """
        Valida formato de email
        """
        if not email:
            return False
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    @staticmethod
    def validate_date_format(date_str: str) -> bool:
        """
        Valida formato de fecha (ISO 8601)
        """
        if not date_str:
            return False
        try:
            datetime.fromisoformat(str(date_str).replace('Z', '+00:00'))
            return True
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def validate_numeric_range(value: Any, min_val: Optional[float] = None, 
                             max_val: Optional[float] = None) -> bool:
        """
        Valida que un valor numérico esté dentro del rango especificado
        """
        try:
            num_value = float(value)
            if min_val is not None and num_value < min_val:
                return False
            if max_val is not None and num_value > max_val:
                return False
            return True
        except (ValueError, TypeError):
            return False
    
    @staticmethod
    def validate_string_length(value: str, max_length: int) -> bool:
        """
        Valida longitud máxima de string
        """
        if not isinstance(value, str):
            return False
        return len(value) <= max_length
    
    @staticmethod
    def validate_required_fields(record: Dict[str, Any], 
                                required_fields: List[str]) -> List[str]:
        """
        Valida que todos los campos requeridos estén presentes y no sean nulos
        """
        missing_fields = []
        for field in required_fields:
            if field not in record or record[field] is None or record[field] == '':
                missing_fields.append(field)
        return missing_fields
    
    @staticmethod
    def validate_record_against_rules(record: Dict[str, Any], 
                                    rules: Dict[str, Any]) -> Dict[str, Any]:
        """
        Valida un registro completo contra un conjunto de reglas
        """
        validation_result = {
            'valid': True,
            'errors': [],
            'warnings': []
        }
        
        # Validar campos requeridos
        if 'required_fields' in rules:
            missing = DataValidators.validate_required_fields(
                record, rules['required_fields']
            )
            if missing:
                validation_result['valid'] = False
                validation_result['errors'].append(
                    f"Campos requeridos faltantes: {', '.join(missing)}"
                )
        
        # Validar longitudes de string
        if 'max_length' in rules:
            for field, max_len in rules['max_length'].items():
                if field in record and record[field] is not None:
                    if not DataValidators.validate_string_length(record[field], max_len):
                        validation_result['valid'] = False
                        validation_result['errors'].append(
                            f"Campo {field} excede longitud máxima de {max_len}"
                        )
        
        # Validar formatos específicos
        if 'format_validations' in rules:
            for field, pattern in rules['format_validations'].items():
                if field in record and record[field] is not None:
                    if not re.match(pattern, str(record[field])):
                        validation_result['valid'] = False
                        validation_result['errors'].append(
                            f"Campo {field} no tiene formato válido"
                        )
        
        # Validar campos de email
        if 'email_field' in rules:
            email_field = rules['email_field']
            if email_field in record and record[email_field] is not None:
                if not DataValidators.validate_email_format(record[email_field]):
                    validation_result['valid'] = False
                    validation_result['errors'].append(
                        f"Email inválido en campo {email_field}"
                    )
        
        # Validar campos numéricos
        if 'numeric_fields' in rules:
            for field in rules['numeric_fields']:
                if field in record and record[field] is not None:
                    try:
                        float(record[field])
                    except (ValueError, TypeError):
                        validation_result['valid'] = False
                        validation_result['errors'].append(
                            f"Campo {field} debe ser numérico"
                        )
        
        # Validar rangos de valores
        if 'min_values' in rules:
            for field, min_val in rules['min_values'].items():
                if field in record and record[field] is not None:
                    if not DataValidators.validate_numeric_range(record[field], min_val=min_val):
                        validation_result['valid'] = False
                        validation_result['errors'].append(
                            f"Campo {field} debe ser mayor o igual a {min_val}"
                        )
        
        if 'max_values' in rules:
            for field, max_val in rules['max_values'].items():
                if field in record and record[field] is not None:
                    if not DataValidators.validate_numeric_range(record[field], max_val=max_val):
                        validation_result['valid'] = False
                        validation_result['errors'].append(
                            f"Campo {field} debe ser menor o igual a {max_val}"
                        )
        
        # Validar campos de fecha
        if 'date_fields' in rules:
            for field in rules['date_fields']:
                if field in record and record[field] is not None:
                    if not DataValidators.validate_date_format(record[field]):
                        validation_result['warnings'].append(
                            f"Campo {field} podría tener formato de fecha inválido"
                        )
        
        return validation_result

class DataTransformations:
    """
    Transformaciones comunes de datos
    """
    
    @staticmethod
    def clean_user_data(record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Limpia y normaliza datos de usuario
        """
        if not record:
            return None
        
        cleaned = record.copy()
        
        # Limpiar email
        if 'Email' in cleaned and cleaned['Email']:
            cleaned['Email'] = cleaned['Email'].strip().lower()
        
        # Limpiar nombre de usuario
        if 'NombreUsuario' in cleaned and cleaned['NombreUsuario']:
            cleaned['NombreUsuario'] = cleaned['NombreUsuario'].strip()
        
        # Capitalizar nombre completo
        if 'NombreCompleto' in cleaned and cleaned['NombreCompleto']:
            cleaned['NombreCompleto'] = cleaned['NombreCompleto'].strip().title()
        
        # Asegurar que Activo sea booleano
        if 'Activo' in cleaned:
            if isinstance(cleaned['Activo'], str):
                cleaned['Activo'] = cleaned['Activo'].lower() in ['true', '1', 'si', 'yes']
            elif cleaned['Activo'] is None:
                cleaned['Activo'] = True
        
        return cleaned
    
    @staticmethod
    def normalize_transaction_data(record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normaliza datos de transacciones
        """
        if not record:
            return None
        
        normalized = record.copy()
        
        # Asegurar que Amount sea decimal con 2 lugares
        if 'Amount' in normalized and normalized['Amount'] is not None:
            try:
                normalized['Amount'] = round(float(normalized['Amount']), 2)
            except (ValueError, TypeError):
                return None  # Registro inválido
        
        # Normalizar fecha
        if 'Date' in normalized and normalized['Date']:
            try:
                if isinstance(normalized['Date'], str):
                    dt = datetime.fromisoformat(normalized['Date'].replace('Z', '+00:00'))
                    normalized['Date'] = dt.isoformat()
            except ValueError:
                pass  # Mantener formato original si no se puede convertir
        
        return normalized
    
    @staticmethod
    def standardize_inventory_data(record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Estandariza datos de inventario
        """
        if not record:
            return None
        
        standardized = record.copy()
        
        # Normalizar nombre del producto
        if 'ProductName' in standardized and standardized['ProductName']:
            standardized['ProductName'] = standardized['ProductName'].strip().title()
        
        # Asegurar que Quantity sea entero
        if 'Quantity' in standardized and standardized['Quantity'] is not None:
            try:
                standardized['Quantity'] = int(float(standardized['Quantity']))
            except (ValueError, TypeError):
                return None
        
        # Asegurar que Price sea decimal
        if 'Price' in standardized and standardized['Price'] is not None:
            try:
                standardized['Price'] = round(float(standardized['Price']), 2)
            except (ValueError, TypeError):
                return None
        
        return standardized
