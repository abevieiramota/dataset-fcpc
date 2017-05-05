# -*- coding: utf-8 -*-
import os
import logging
import pandas as pd
import re
from sklearn.preprocessing import LabelEncoder


class PagamentosPessoaFisicaExtractor:

    READ_CFG = {
        "decimal": ",",
        "thousands": ".",
    }

    CAT_COLS = ['CPF',
                'NOME',
                'TIPO DE PAGAMENTO',
                'PROJETO']

    CAT_ID_COLS = [colname + "-ID" for colname in CAT_COLS]

    def __init__(self):

        self.logger = logging.getLogger("PagamentosPessoaFisicaExtractor")
        
        self.files = [filename for filename in os.listdir(".") if filename.endswith(".zip")]

        self.df = None
        self.encoders = {}


    def ler_zip_csv(self, filepath):

        self.logger.info('>Processando arquivo {}'.format(filepath))
        
        return pd.read_csv(filepath, **PagamentosPessoaFisicaExtractor.READ_CFG)

    def extract_data(self):

        self.logger.info("EXTRAINDO OS DADOS")

        dfs = [self.ler_zip_csv(zip_file) for zip_file in self.files]

        self.df = pd.concat(dfs, ignore_index=True).fillna("NÃO-ESPECIFICADO")

    def make_encoders(self):

        for (colname, id_colname) in zip(PagamentosPessoaFisicaExtractor.CAT_COLS, PagamentosPessoaFisicaExtractor.CAT_ID_COLS):

            self.logger.info(">Normalizando coluna {}".format(colname))

            encoder = LabelEncoder().fit(self.df[colname].unique())
        
            encoder_df = pd.DataFrame(encoder.classes_, columns=[colname])
            encoder_df.index.name = id_colname
            encoder_df.to_csv(colname + ".csv", encoding='utf-8')
            
            self.encoders[colname] = encoder

        
    def normalize(self):

        self.logger.info("NORMALIZANDO COLUNAS")

        self.make_encoders()

        self.df[PagamentosPessoaFisicaExtractor.CAT_ID_COLS] = self.df[PagamentosPessoaFisicaExtractor.CAT_COLS]\
        .apply(lambda col: self.encoders[col.name].transform(col))
        self.df.drop(PagamentosPessoaFisicaExtractor.CAT_COLS, axis=1, inplace=True)


    def save_df(self):

        self.logger.info("SALVANDO O DATASET FINAL")

        self.df.to_csv("dataset.csv", index=False, encoding='utf-8')


    def processar(self):

        self.logger.info('CRIANDO DATASET FINAL')

        self.extract_data()
        self.normalize()
        self.save_df()


if __name__ == "__main__":

    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    ppfe = PagamentosPessoaFisicaExtractor()

    ppfe.processar()