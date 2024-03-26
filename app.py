import subprocess

def runcmd(cmd, verbose = False, *args, **kwargs):
    process = subprocess.Popen(
        cmd,
        stdout = subprocess.PIPE,
        stderr = subprocess.PIPE,
        text = True,
        shell = True
    )
    std_out, std_err = process.communicate()
    if verbose:
        print(std_out.strip(), std_err)
    pass

URL_BASE = "https://dadosabertos.ans.gov.br/FTP/Base_de_dados/Microdados/dados_dbc/"
# Requisição para obtenção dos dados com base nos parâmetros
runcmd(f"wget -r -np -R 'index.html*' {URL_BASE}", verbose=True)
