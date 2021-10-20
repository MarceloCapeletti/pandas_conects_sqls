
"""

Pandas para conexão com banco de dados Oracle

@author: Marcelo Bruno Capeletti

"""

import cx_Oracle
import pandas as pd
import sys


class PNT_sql:
    
    def __init__(self):
        
        self.df_ora = pd.DataFrame()
        
        '''
            Localizar arquivo instantclient_19_10
            
            Site para download:
            https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html#installing-cx-oracle-on-windows
        '''
          
         #Localizar arquivo instantclient_19_10
        instant_client_dir = r"C:\diretorio\instantclient_19_10" 
        
        # Iniciar somente uma vez o cliente por seção
        if sys.platform.startswith("darwin"): 
            cx_Oracle.init_oracle_client(lib_dir=instant_client_dir)
        
    def get_sql(self,query):
        
        try:
            
            # DNS/ip Servidor Oracle
            dsn_tns = cx_Oracle.makedsn('server', 'port') 
            
            connection = cx_Oracle.connect("login", "senha",dsn=dsn_tns,encoding="UTF-8")
            
            # Query  no banco de dados Oracle
            df_ora = pd.read_sql(query, connection) 
            
            # Exportar Tabela
            df_ora.to_csv(r'nome_tabela.csv') 
            
        except Exception as err:
            
            print("Erro! Começar novamente!")
            print(err);
            
            sys.exit(1); 
            
        finally:

            connection.close()
            
        return df_ora
        

if __name__ == '__main__':
    
    # Query 
    query = """  

            """   
            
    PNT_sql = PNT_sql()
    df_ora = PNT_sql.get_sql(query)
