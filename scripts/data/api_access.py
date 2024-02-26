""" Api Access Module"""

import requests
from pathlib import Path
from scripts.data.secrets.api_access_data import PATH_ACCESS_TOKEN, PATH_REFRESH_TOKEN

def get_access_token(client_secret: str, app_id: str, code_user: str, redirect_uri: str) -> dict:
    """Get access token from MercadoLibre API
    It is only required once to obtain the access token and the refresh token, 
    the secrets folder (scripts/data/secrets/access_token.txt) will be updated with the access token.
    For a new access token, use the refresh_access_token method.
    
    See: https://developers.mercadolibre.com.ar/es_ar/autenticacion-y-autorizacion/#Autorizaci%C3%B3n
    
    Args:
        client_secret (str): Client secret from the app.
        app_id (str): App id from the app.
        code_user (str): Code user generated after the user login.
        redirect_uri (str): Redirect URI from the app.

    Returns:
        response (dict): Response from the API.
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
        
    with open(PATH_REFRESH_TOKEN, "w") as file:
        file.write(response["refresh_token"])

    return response

def refresh_access_token(client_secret: str, app_id: str, refresh_token: str) -> str:
    """Generate a new access token from the refresh token and update the access_token file in
    the secrets folder (scripts/data/secrets/access_token.txt).
    
    See: https://developers.mercadolibre.com.ar/es_ar/autenticacion-y-autorizacion/#Refresh-token

    Args:
        client_secret (str): Client secret from the app.
        app_id (str): App id from the app.
        refresh_token (str): Refresh token from the get_access_token method.

    Returns:
        response["access_token"] (str): String with the new access token.
    """
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

    return response["access_token"]

def call_access_token()->str:
    """Call access token string from the secrets folder (scripts/data/secrets/access_token.txt).
    Returns:
        str: Aceess token string.
    """
    with open(PATH_ACCESS_TOKEN, "r") as file:
        return file.read()
    
def call_refresh_token()->str:
    """Call refresh token string from the secrets folder (scripts/data/secrets/refresh_token.txt).
    Returns:
        str: refresh token string.
    """
    with open(PATH_REFRESH_TOKEN, "r") as file:
        return file.read()

