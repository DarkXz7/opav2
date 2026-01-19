from django import template

register = template.Library()

@register.filter
def get_item(dictionary, key):
    """
    Obtiene un elemento de un diccionario usando una clave.
    Uso: {{ dict|get_item:key }}
    """
    if not dictionary:
        return None
    return dictionary.get(key)