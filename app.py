"""
Script para obtencao dos dados da ANS em .dbc
e conversao para .dbf
"""

from bs4 import BeautifulSoup
from pyreaddbc.readdbc import dbc2dbf
import requests
import os

def is_directory(url: str) -> bool:
    """
    Verificar se URL e um diretorio

    Arg: url (URL para verificacao)
    Ret: bool
    """
    if url.endswith("/"):
        return True
    else:
        return False

def fetch_data(url: str, path: str) -> None:
    """
    Obtencao e conversao dos dados

    Args: url (URL para download), path (caminho relativo para salvamento)
    Ret: None
    """
    page = requests.get(url, timeout=10).content
    bs_obj = BeautifulSoup(page, "html.parser")
    possible_dirs = bs_obj.findAll("a", href=True)

    for link in possible_dirs:
        if link["href"].startswith("/FTP/"):
            continue
        elif is_directory(link["href"]):
            curr_dir = os.path.abspath(os.path.join(path, link["href"]))
            new_url = url + link["href"]
            os.makedirs(curr_dir, exist_ok=True)

            fetch_data(new_url, curr_dir)
        else:
            if link["href"].endswith(".dbc"):
                out = os.path.abspath(os.path.join(path, link["href"]))
                res = requests.get(f"{url}/{link['href']}", timeout=10)

                open(out, "wb").write(res.content)
                dbc2dbf(out, f"{out[:-3]}dbf")
                os.remove(out)
                print(f"{out[:-3]}dbf: {res.status_code}")

root_dir = os.getcwd()
os.makedirs(os.path.abspath(os.path.join(root_dir, "Dados")), exist_ok=True)
root_dir = os.path.abspath(os.path.join(root_dir, "Dados"))

URL_BASE = "https://dadosabertos.ans.gov.br/FTP/Base_de_dados/Microdados/dados_dbc/"

fetch_data(URL_BASE, root_dir)
