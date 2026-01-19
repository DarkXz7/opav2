"""
Servicio para integración con OneDrive
Maneja autenticación, descarga de archivos y sincronización
"""

import os
import requests
from io import BytesIO
from django.conf import settings
import logging

logger = logging.getLogger(__name__)


class OneDriveService:
    """
    Servicio para interactuar con OneDrive.
    
    Actualmente soporta descargas directas usando URLs compartidas.
    Para autenticación OAuth2 completa, ver configuración en settings.py
    """
    
    def __init__(self):
        """
        Inicializar el servicio OneDrive
        Requiere configuración en settings.py:
        - ONEDRIVE_CLIENT_ID
        - ONEDRIVE_CLIENT_SECRET
        - ONEDRIVE_REDIRECT_URI
        """
        self.client_id = getattr(settings, 'ONEDRIVE_CLIENT_ID', None)
        self.client_secret = getattr(settings, 'ONEDRIVE_CLIENT_SECRET', None)
        self.redirect_uri = getattr(settings, 'ONEDRIVE_REDIRECT_URI', None)
        self.access_token = None
    
    def download_file_from_url(self, share_url):
        """
        Descarga un archivo de OneDrive desde una URL compartida.
        
        Args:
            share_url (str): URL compartida del archivo en OneDrive
                           Ej: https://1drv.ms/x/s!AxxxBxxxCxx
        
        Returns:
            BytesIO: Contenido del archivo en memoria, listo para pandas
        
        Raises:
            Exception: Si la descarga falla
        """
        try:
            logger.info(f"Descargando archivo de OneDrive: {share_url}")
            
            # Convertir URL compartida a URL de descarga directa
            download_url = self._convert_share_url_to_download_url(share_url)
            
            # Descargar el archivo
            response = requests.get(download_url, timeout=30)
            response.raise_for_status()
            
            # Retornar contenido en BytesIO
            file_content = BytesIO(response.content)
            logger.info("Archivo descargado exitosamente de OneDrive")
            
            return file_content
            
        except Exception as e:
            logger.error(f"Error descargando archivo de OneDrive: {str(e)}")
            raise
    
    def _convert_share_url_to_download_url(self, share_url):
        """
        Convierte una URL compartida de OneDrive a una URL de descarga directa.
        
        Ejemplo:
            Input:  https://1drv.ms/x/s!AxxxBxxxCxx
            Output: https://1drv.ms/download?resid=...&authkey=...
        
        Args:
            share_url (str): URL compartida
        
        Returns:
            str: URL de descarga directa con parámetro download=1
        """
        # Si ya es una URL de descarga, retornarla directamente
        if 'download=1' in share_url or '/download?' in share_url:
            return share_url
        
        # Para URLs de tipo 1drv.ms, agregar ?download=1
        if '1drv.ms' in share_url:
            if '?' in share_url:
                return share_url + '&download=1'
            else:
                return share_url + '?download=1'
        
        # Para URLs de tipo outlook.live.com/view.aspx
        if 'outlook.live.com' in share_url:
            return share_url.replace('view.aspx', 'download.aspx')
        
        # Por defecto, agregar ?download=1
        if '?' in share_url:
            return share_url + '&download=1'
        else:
            return share_url + '?download=1'
    
    def get_file_metadata(self, item_id):
        """
        Obtiene metadatos de un archivo en OneDrive.
        
        Requiere:
        - Estar autenticado con OAuth2
        - Access token válido
        
        Args:
            item_id (str): ID del item en OneDrive
        
        Returns:
            dict: Información del archivo (nombre, tamaño, fecha, etc.)
        """
        if not self.access_token:
            raise ValueError("No autenticado. Requiere OAuth2 token.")
        
        url = f"https://graph.microsoft.com/v1.0/me/drive/items/{item_id}"
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error obteniendo metadata de OneDrive: {str(e)}")
            raise
    
    def validate_share_url(self, share_url):
        """
        Valida que una URL compartida sea accesible.
        Realiza una solicitud HEAD para verificar sin descargar todo.
        
        Args:
            share_url (str): URL compartida del archivo
        
        Returns:
            bool: True si la URL es válida y accesible
        """
        try:
            download_url = self._convert_share_url_to_download_url(share_url)
            response = requests.head(download_url, timeout=10, allow_redirects=True)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"URL de OneDrive no válida: {str(e)}")
            return False


# Función auxiliar para obtener la instancia del servicio
def get_onedrive_service():
    """Retorna una instancia del servicio OneDrive"""
    return OneDriveService()
