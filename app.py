from bs4 import BeautifulSoup
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

def fetch_data(url: str, out_path: str) -> None:
    page = requests.get(url, timeout=10).content
    bs_obj = BeautifulSoup(page, "html.parser")
    possible_dirs = bs_obj.findAll("a", href=True)

    for link in possible_dirs:
        if is_directory(link["href"]):
            curr_dir = os.path.abspath(os.path.join(out_path, link["href"]))
            new_url = url + link["href"]
            os.makedirs(curr_dir, exist_ok=True)

            fetch_data(new_url, curr_dir)
        else:
            if link["href"].endswith(".dbc"):
                out = os.path.abspath(os.path.join(out_path, link["href"]))
                res = requests.get(f"{url}/{link["href"]}", timeout=10)

                open(out, "wb").write(res.content)
                print(f"{out}: {res.status_code}")

root_dir = os.getcwd()
os.makedirs(os.path.abspath(os.path.join(root_dir, "Dados")), exist_ok=True)
root_dir = os.path.abspath(os.path.join(root_dir, "Dados"))

URL_BASE = "https://dadosabertos.ans.gov.br/FTP/Base_de_dados/Microdados/dados_dbc/"

fetch_data(URL_BASE, root_dir)
