import requests
from pathlib import Path
from scripts.data.secrets.api_access_data import PATH_ACCESS_TOKEN

def get_access_token(client_secret, app_id, code_user, redirect_uri):
    """Get access token from MercadoLibre API
    Solo se requiere una promera vez para obtener el access token y el refresh token.
    See: https://developers.mercadolibre.com.ar/es_ar/autenticacion-y-autorizacion/
    

    Args:
        client_secret (_type_): _description_
        app_id (_type_): _description_
        code_user (_type_): _description_
        redirect_uri (_type_): _description_

    Returns:
        _type_: _description_
    """
    url = "https://api.mercadolibre.com/oauth/token"
    payload = f'grant_type=authorization_code&client_id={app_id}&client_secret={client_secret}&code={code_user}&redirect_uri={redirect_uri}'
    headers = {
      'accept': 'application/json',
      'content-type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    response = response.json()
    with open(PATH_ACCESS_TOKEN, "w") as file:
        file.write(response["access_token"])

    return response

def refresh_access_token(client_secret, app_id, refresh_token):
    url = "https://api.mercadolibre.com/oauth/token"
    payload = f'grant_type=refresh_token&client_id={app_id}&client_secret={client_secret}&refresh_token={refresh_token}'
    headers = {
      'accept': 'application/json',
      'content-type': 'application/x-www-form-urlencoded'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response = response.json()
    with open(PATH_ACCESS_TOKEN, "w") as file:
        file.write(response["access_token"])

    return response

def call_access_token():
    with open(PATH_ACCESS_TOKEN, "r") as file:
        return file.read()

