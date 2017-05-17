import pandas as pd
import numpy as np
from numpy import dtype
import os

fcpc_dtypes = {'CPF-ID': dtype('int64'),
 'DATA': dtype('O'),
 'NOME-ID': dtype('int64'),
 'PROJETO-ID': dtype('int64'),
 'TIPO DE PAGAMENTO-ID': dtype('int64'),
 'VALOR (R$)': dtype('float64')}

def read_fcpc(basepath='.'):

    # leitura de arquivo .CSV
    fcpc = pd.read_csv(os.path.join(basepath, "dataset.csv"), 
                        dtype=fcpc_dtypes,
                        parse_dates=["DATA"])

    # reconstitui as colunas normalizadas
    for fk in (col for col in fcpc.columns if col.endswith("-ID")):
        
        filepath = os.path.join(basepath, 
                                "{}.csv".format(fk.rsplit("-", maxsplit=1)[0]))
        col_df = pd.read_csv(filepath)
        fcpc = pd.merge(fcpc, col_df)
        fcpc.drop(fk, axis=1, inplace=True)
    
    return fcpc

