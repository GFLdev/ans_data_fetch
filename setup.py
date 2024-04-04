import sys
from cx_Freeze import setup, Executable

# Dependências do projeto
packages = [
    "PyQt5",
    "os",
    "sys",
    "subprocess",
    "bs4",
    "requests"
]

options = {
    "build_exe": {
        "packages": packages
    }
}

BASE = None
if sys.platform == "win32":
    BASE = "Win32GUI"

executables = [
    Executable(
        "app.py",
        base=BASE,
        target_name="ANS DBC2DBF"
    )
]

setup(
    name="ANS Data Fetch",
    version="1.0.0",
    description="Software para obtenção dos dados da ANS e conversão dos arquivos para .dbf",
    options=options,
    executables=executables
)
