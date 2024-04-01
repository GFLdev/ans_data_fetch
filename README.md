# ANS Data Fetch

Software para obtenção dos dados da ANS e conversão dos arquivos para .dbf

**Obs:** É necessário o uso do Google Colab, caso não utilize Linux

# Dependências

- PySUS (disponível apenas para Linux e Google Colab)
- BeautifulSoup 4
- Requests

# Run

## Linux

```bash
git clone https://github.com/GFLdev/ans_data_fetch
cd ans_data_fetch
pip install PySUS bs4 requests
python app.py
```

## Google Colab
- Adicione uma célula anterior ao código para instalação do PySUS

    ```
    !pip install pysus
    ```

- Após isto, basta criar uma outra célula e colar o código de app.py