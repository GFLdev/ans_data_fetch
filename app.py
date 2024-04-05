"""
Script para obtencao dos dados da ANS em .dbc
e conversao para .dbf
"""

import os
import subprocess
import sys
from bs4 import BeautifulSoup
import requests
from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QMessageBox
from PyQt5.QtWidgets import QVBoxLayout, QPushButton, QFileDialog, QLabel
from PyQt5.QtCore import Qt, QThread, pyqtSignal

URL_BASE = "https://dadosabertos.ans.gov.br/FTP/Base_de_dados/Microdados/dados_dbc/"

class WorkerThread(QThread):
    """
    Definição da classe
    """
    finished = pyqtSignal()

    def __init__(self, root_dir) -> None:
        super().__init__()

        self.root_dir = root_dir

    def is_directory(self, url: str) -> bool:
        """
        Verificar se URL e um diretorio

        Arg: url (URL para verificacao)
        Ret: bool
        """
        if url.endswith("/"):
            return True
        else:
            return False

    def fetch_data(self, url: str, path: str) -> None:
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
            elif self.is_directory(link["href"]):
                curr_dir = os.path.abspath(os.path.join(path, link["href"]))
                new_url = url + link["href"]
                os.makedirs(curr_dir, exist_ok=True)

                self.fetch_data(new_url, curr_dir)
            else:
                if link["href"].endswith(".dbc"):
                    if not os.path.exists(
                        os.path.join(
                            path,
                            f"{link['href'][:-3]}dbf"
                        )
                    ):
                        out = os.path.abspath(os.path.join(path, link["href"]))
                        res = requests.get(f"{url}/{link['href']}", timeout=10)

                        open(out, "wb").write(res.content)
                        subprocess.run(
                            ["./Tab415/dbf2dbc.exe", out, path],
                            check = True
                        )
                        os.remove(out)
                        print(f"{out[:-3]}dbf: {res.status_code}")

    def run(self) -> None:
        """
        Execução do script
        """
        os.makedirs(os.path.abspath(os.path.join(self.root_dir, "Dados")), exist_ok=True)
        self.root_dir = os.path.abspath(os.path.join(self.root_dir, "Dados"))

        self.fetch_data(URL_BASE, self.root_dir)
        self.finished.emit()

class App(QMainWindow):
    """
    Criação da classe para QMainWindow do GUI
    """
    def __init__(self):
        super().__init__()

        self.root_dir = ""
        self.worker_thread = None

        self.init_ui()

    def init_ui(self) -> None:
        """
        Inicializador do GUI
        """
        self.setWindowTitle("Dados ANS")

        layout = QVBoxLayout()
        layout.setAlignment(Qt.AlignCenter)
        layout.setContentsMargins(20, 20, 20, 20)

        self.btn_dir = QPushButton("Selecionar pasta", self)
        self.btn_dir.clicked.connect(self.set_dir)
        layout.addWidget(self.btn_dir)

        self.lbl_dir = QLabel("Nenhum diretório selecionado", self)
        self.lbl_dir.setContentsMargins(0, 5, 0, 15)
        layout.addWidget(self.lbl_dir)

        self.btn_process = QPushButton("Processar", self)
        self.btn_process.clicked.connect(self.process)
        self.btn_process.setEnabled(False)
        layout.addWidget(self.btn_process)

        self.central_widget = QWidget()
        self.central_widget.setLayout(layout)
        self.setCentralWidget(self.central_widget)

        self.adjustSize()
        self.setFixedHeight(self.height())
        self.setFixedWidth(300)


    def process(self):
        """
        Fila de processos
        """
        if self.worker_thread is None or not self.worker_thread.isRunning():
            self.worker_thread = WorkerThread(self.root_dir)
            self.worker_thread.finished.connect(self.worker_thread.deleteLater)
            self.worker_thread.finished.connect(self.handle_finished)
            self.btn_process.setText("Processando")
            self.btn_dir.setEnabled(False)
            self.btn_process.setEnabled(False)
            self.worker_thread.start()

    def handle_finished(self):
        """
        Redefinição da interface e variáveis, e caixa de diálogo
        """
        QMessageBox.information(
            None,
            "Sucesso",
            f"Dados extraídos com sucesso em:\n{self.root_dir}/Dados"
        )

        self.root_dir = ""
        self.lbl_dir.setText("Nenhum diretório selecionado")
        self.btn_process.setText("Processar")
        self.btn_dir.setEnabled(True)

    def set_dir(self) -> None:
        """
        Seleção da pasta para salvamento dos arquivos
        """
        directory = QFileDialog.getExistingDirectory(
            self,
            "Pasta para salvamento dos arquivos",
            "/"
        )

        if directory:
            self.root_dir = directory
            self.lbl_dir.setText(self.root_dir)
            self.btn_process.setEnabled(True)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setStyleSheet("""
        * {
            font-size: 14px;
        }

        QPushButton {
            padding: 0.5em 1em;
            text-align: center;
        }
    """)
    ex = App()
    ex.show()
    sys.exit(app.exec_())
