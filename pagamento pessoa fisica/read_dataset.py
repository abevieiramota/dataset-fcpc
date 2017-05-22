import pandas as pd
from numpy import dtype
import os

fcpc_dtypes = {'CPF-ID': dtype('int64'),
 'DATA': dtype('O'),
 'NOME-ID': dtype('int64'),
 'PROJETO-ID': dtype('int64'),
 'TIPO DE PAGAMENTO-ID': dtype('int64'),
 'VALOR (R$)': dtype('float64')}

basepath = os.path.dirname(os.path.abspath(__file__))

def read_fcpc(basepath=basepath):

    # leitura de arquivo .CSV
    fcpc = pd.read_csv(os.path.join(basepath, "dataset.csv"), 
                        dtype=fcpc_dtypes,
                        parse_dates=["DATA"])

    # reconstitui as colunas normalizadas
    for fk in (col for col in fcpc.columns if col.endswith("-ID")):
        
        colname = fk.rsplit("-", maxsplit=1)[0]
        filepath = os.path.join(basepath, "{}.csv".format(colname))
        col_df = pd.read_csv(filepath)
        fcpc = pd.merge(fcpc, col_df)
        fcpc.drop(fk, axis=1, inplace=True)
        
    # por algum motivo essa coluna não está sendo reconhecida como string...
    fcpc["PROJETO"] = fcpc.PROJETO.astype("str")
    
    return fcpc

