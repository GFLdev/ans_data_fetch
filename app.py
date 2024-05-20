"""
Script para obtenção dos dados da ANS em DBC
e conversao para DBF
"""
import os
import threading

import requests
import subprocess
import sys
import pandas as pd
from multiprocessing.pool import ThreadPool as Pool
from simpledbf import Dbf5
from threading import Thread
from bs4 import BeautifulSoup
from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QMessageBox
from PyQt5.QtWidgets import QGridLayout, QPushButton, QFileDialog, QLabel, QRadioButton
from PyQt5.QtCore import Qt

URL_BASE = 'https://dadosabertos.ans.gov.br/FTP/Base_de_dados/Microdados/dados_dbc/'


def is_directory(url: str) -> bool:
    """
    Verificar se URL é um diretório

    :param url: URL para verificação
    :return: Bool
    """
    if url.endswith('/'):
        return True
    else:
        return False


class App(QMainWindow):
    """
    Criação da classe para QMainWindow do GUI
    """
    def __init__(self):
        super().__init__()

        self.process_type = True

        self.root_dir = ''
        self.union_files = []
        self.worker_thread = None
        self.process_lock = [0, 0]
        self.res_df = pd.DataFrame()

        self.layout = QGridLayout()
        self.lbl_select = QLabel('Selecione o tipo de modificador: ', self)
        self.lbl_fetch = QLabel('Obter DBF', self)
        self.lbl_union = QLabel('Unir DBF\'s', self)
        self.btn_dir = QPushButton('Selecionar pasta', self)
        self.lbl_dir = QLabel('Nenhum diretório selecionado', self)
        self.btn_process = QPushButton('Processar', self)
        self.central_widget = QWidget()

        self.__init_ui__()

    def __init_ui__(self) -> None:
        """
        Inicializador do GUI
        
        :return: None
        """
        self.setWindowTitle('Dados ANS')

        self.layout.setAlignment(Qt.AlignTop | Qt.AlignHCenter)
        self.layout.setContentsMargins(20, 20, 20, 20)
        self.setFixedSize(300, 300)

        self.lbl_select.setAlignment(Qt.AlignTop | Qt.AlignCenter)
        self.lbl_select.setContentsMargins(0, 5, 0, 15)
        self.layout.addWidget(self.lbl_select, 0, 0, 1, 2)
    
        self.lbl_fetch.setAlignment(Qt.AlignTop | Qt.AlignCenter)
        self.lbl_fetch.setContentsMargins(0, 5, 0, 15)
        self.layout.addWidget(self.lbl_fetch, 1, 0, 1, 1)
    
        radiobutton = QRadioButton(self)
        radiobutton.type = 'fetch'
        radiobutton.toggle()
        radiobutton.toggled.connect(self.__select_type__)
        self.layout.addWidget(radiobutton, 1, 1, 1, 1)
    
        self.lbl_union.setAlignment(Qt.AlignTop | Qt.AlignCenter)
        self.lbl_union.setContentsMargins(0, 5, 0, 15)
        self.layout.addWidget(self.lbl_union, 2, 0, 1, 1)
    
        radiobutton = QRadioButton(self)
        radiobutton.type = 'union'
        radiobutton.toggled.connect(self.__select_type__)
        self.layout.addWidget(radiobutton, 2, 1, 1, 1)

        self.btn_dir.clicked.connect(self.__set_fetch_dir__)
        self.layout.addWidget(self.btn_dir, 3, 0, 1, 2)

        self.lbl_dir.setContentsMargins(0, 5, 0, 15)
        self.layout.addWidget(self.lbl_dir, 4, 0, 1, 2)

        self.btn_process.clicked.connect(self.__process__)
        self.btn_process.setEnabled(False)
        self.layout.addWidget(self.btn_process, 5, 0, 1, 2)

        self.central_widget.setLayout(self.layout)
        self.setCentralWidget(self.central_widget)

        self.adjustSize()
        self.setFixedHeight(self.height())
        self.setFixedWidth(300)

    def __select_type__(self) -> None:
        """
        Método para escolha do tipo de processamento

        :return: None
        """
        radiobutton = self.sender()
        if radiobutton.isChecked():
            self.btn_dir.clicked.disconnect()
            
            if radiobutton.type == 'fetch':
                self.btn_dir.clicked.connect(self.__set_fetch_dir__)
                self.process_type = True
            else:
                self.btn_dir.clicked.connect(self.__set_union_files__)
                self.process_type = False

            self.process_lock[0] = 1

    def __check_lock__(self) -> None:
        """
        Verificação e ativação do botão de processamento e cálculo

        :return: None
        """
        if all(self.process_lock):
            self.btn_process.setEnabled(True)
        else:
            self.btn_process.setEnabled(False)

    def __fetch_data__(self, url: str, path: str) -> None:
        """
        Obtenção e conversão dos dados

        :param url: URL para download
        :param path: Caminho relativo para salvamento
        :return: None
        """
        page = requests.get(url, timeout=10).content
        bs_obj = BeautifulSoup(page, 'html.parser')
        possible_dirs = bs_obj.findAll('a', href=True)

        for link in possible_dirs:
            if link['href'].startswith('/FTP/'):
                continue
            elif is_directory(link['href']):
                curr_dir = os.path.abspath(os.path.join(path, link['href']))
                new_url = url + link['href']
                os.makedirs(curr_dir, exist_ok=True)

                self.__fetch_data__(new_url, curr_dir)
            else:
                if link['href'].endswith('.dbc'):
                    if not os.path.exists(
                            os.path.join(
                                path,
                                f'{link['href'][:-3]}dbf'
                            )
                    ):
                        out = os.path.abspath(os.path.join(path, link['href']))
                        res = requests.get(f'{url}/{link['href']}', timeout=10)

                        open(out, 'wb').write(res.content)
                        subprocess.run(
                            ['./Tab415/dbf2dbc.exe', out, path],
                            check=True
                        )
                        os.remove(out)
                        print(f'{out[:-3]}dbf: {res.status_code}')

    def __callstack__(self) -> None:
        """
        Fila de processos
        
        :return: None
        """
        if self.process_type:
            self.root_dir = os.path.abspath(os.path.join(self.root_dir, 'Dados'))
            os.makedirs(self.root_dir, exist_ok=True)
            self.__fetch_data__(URL_BASE, self.root_dir)
        else:
            self.__union_dbf__()

        self.root_dir = ''
        self.lbl_dir.setText('Nenhum diretório selecionado')
        self.btn_process.setText('Processar')
        for i in range(self.layout.count()):
            widget = self.layout.itemAt(i).widget()
            if widget:
                widget.setEnabled(True)
        self.process_lock = [0, 0]
        self.__check_lock__()

    def __process__(self) -> None:
        """
        Execução do callstack
        
        :return: None
        """
        t1 = Thread(target=self.__callstack__)
        for i in range(self.layout.count()):
            widget = self.layout.itemAt(i).widget()
            if widget:
                widget.setEnabled(False)
        self.btn_process.setText('Processando')
        t1.start()

    def __set_fetch_dir__(self) -> None:
        """
        Seleção da pasta para salvamento dos arquivos

        :return: None
        """
        directory = QFileDialog.getExistingDirectory(
            self,
            'Pasta para salvamento dos arquivos',
            '/'
        )

        if directory:
            self.root_dir = directory
            self.lbl_dir.setText(self.root_dir)
            self.process_lock[1] = 1
            self.__check_lock__()

    def __set_union_files__(self) -> None:
        """
        Seleção dos arquivos DBF para união

        :return: None
        """
        files = QFileDialog().getOpenFileNames(
            self,
            'Arquivos DBF',
            '~/',
            'DBF (*.dbf)'
        )

        if files:
            self.union_files = files
            self.lbl_dir.setText(self.root_dir)
            self.process_lock[1] = 1
            self.__check_lock__()

    def __union_op__(self, file) -> None:
        """
        Processo de união de arquivos DBF para multiprocessamento

        :param file: Arquivo DBF para união
        :return: None
        """
        try:
            dbf = Dbf5(file)
            df = dbf.to_dataframe()
            self.res_df = pd.concat([self.res_df, df])
        except Exception as e:
            QMessageBox.Warning(
                None,
                'Erro',
                f'[ERR] Erro ao ler o arquivo em "{file}":\n{e}/'
            )

    def __union_dbf__(self) -> None:
        """
        Une arquivos DBF em um único arquivo JSON

        :return: None
        """
        # Inicializa uma lista para armazenar os DataFrames
        self.res_df = pd.DataFrame()

        # Percorre todos os arquivos DBF na pasta
        pool = Pool(threading.active_count())

        results = [pool.apply_async(self.__union_op__, (filepath,)) for filepath in self.union_files[0]]

        self.union_files = []

        pool.close()
        pool.join()

        # Verifica os resultados
        for result in results:
            result.get()

        # Verifica se a lista de DataFrames está vazia
        if self.res_df.empty:
            QMessageBox.information(
                None,
                'Processamento concluído',
                'Nenhum arquivo .dbf foi encontrado ou lido com sucesso.'
            )
        else:
            self.res_df.sort_values(
                self.res_df.columns[0],
                ignore_index=True,
                inplace=True
            )

            # Salva o DataFrame combinado em um arquivo JSON
            file_path, _ = QFileDialog.getSaveFileName(
                self,
                'Salvar Arquivo',
                'Uniao.json',
                'JavaScript Object Notation (*.json);;Todos os Arquivos (*)'
            )

            if file_path:
                self.res_df.to_json(
                    f'{file_path}',
                    orient='records'
                )

                QMessageBox.information(
                    None,
                    'Sucesso',
                    f'Arquivos DBF unidos com sucesso e salvo em:\n{file_path}'
                )


if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
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
