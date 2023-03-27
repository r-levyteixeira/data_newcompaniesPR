import pandas as pd
from os import listdir
from os.path import isfile, join
import re
import wget
import zipfile


#Download data from Brazilian government
#For manual download or information see https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-jurdica---cnpj available only in portuguese
#Information about the datasets can be found at https://www.gov.br/receitafederal/dados/cnpj-metadados.pdf or in the link above
print("Downloading zip files...\n")
for i in range(10):
    wget.download('https://dadosabertos.rfb.gov.br/CNPJ/Empresas'+str(i)+'.zip')
    wget.download('https://dadosabertos.rfb.gov.br/CNPJ/Estabelecimentos'+str(i)+'.zip')

#Extra all files from .zip into the main data folder
print("\nExtracting files...\n")   
zipfiles = [f for f in listdir() if isfile(f) and re.search("[^'']*"'.zip$',f)!=None]
for f in zipfiles:
    with zipfile.ZipFile(f, 'r') as zip_ref:
        zip_ref.extractall()

#Read all files .estabele and .emprecsv, basically a table without header and semi-colon separator 
#Following the dataset guidelines the column names are in portuguese. More information can be obtained from instructions file.
#Here we choose a particular state 'PR', but the same could be done for any other state 

colnames_estab=['CNPJ_BASICO','CNPJ_ORDEM','CNPJ_DV','IDENTIFICADOR_MATRIZ/FILIAL','NOME_FANTASIA','SITUACAO_CADASTRAL','DATA_SITUACAO_CADASTRAL','MOTIVO_SITUACAO_CADASTRAL','NOME_DA_CIDADE_NO_EXTERIOR','PAIS','DATA_DE_INICIO_ATIVIDADE','CNAE_FISCAL_PRINCIPAL','CNAE_FISCAL_SECUNDARIA','TIPO_DE_LOGRADOURO','LOGRADOURO','NUMERO','COMPLEMENTO','BAIRRO','CEP','UF','MUNICIPIO','DDD_1','TELEFONE_1','DDD_2','TELEFONE_2','DDD_DO_FAX','FAX','CORREIO_ELETRONICO','SITUACAO_ESPECIAL','DATA_DA_SITUACAO_ESPECIAL'];
keep_column_estabele=['CNPJ_BASICO','CNPJ_ORDEM','IDENTIFICADOR_MATRIZ/FILIAL','NOME_FANTASIA','SITUACAO_CADASTRAL','DATA_SITUACAO_CADASTRAL','DATA_DE_INICIO_ATIVIDADE','CNAE_FISCAL_PRINCIPAL','UF','MUNICIPIO','CORREIO_ELETRONICO']

data_estab = pd.DataFrame(columns=keep_column_estabele)
estabelefiles = [f for f in listdir() if isfile(f) and re.search("[^'']*"'.ESTABELE$',f)!=None];
print("Reading "+str(len(estabelefiles))+" .estabele files...\n")  
for f in estabelefiles:
    print("Reading file: "+str(f)+"...\n") 
    temp_df= pd.read_csv(f, encoding = "ISO-8859-1",sep=';',names=colnames_estab,header=None)
    data_estab = pd.concat([data_estab,temp_df[temp_df['UF']=='PR'][keep_column_estabele]])
    
colnames_empre=['CNPJ_BASICO','RAZAO_SOCIAL','NATUREZA_JURIDICA','QUALIFICACAO_DO_RESPONSAVEL','CAPITAL_SOCIAL_DA_EMPRESA','PORTE_DA_EMPRESA','ENTE_FEDERATIVO_RESPONSAVEL']
keep_column_empre = ['CNPJ_BASICO','NATUREZA_JURIDICA','CAPITAL_SOCIAL_DA_EMPRESA','PORTE_DA_EMPRESA']

data_empre = pd.DataFrame(columns=keep_column_empre)
emprefiles = [f for f in listdir() if isfile(f) and re.search("[^'']*"'.EMPRECSV$',f)!=None];
print("\nReading "+str(len(emprefiles))+" .emprecsv files...\n")  
for f in emprefiles:
    print("Reading file: "+str(f)+"...\n") 
    temp_df= pd.read_csv(f, encoding = "ISO-8859-1",sep=';',names=colnames_empre,header=None)
    data_empre = pd.concat([data_empre,temp_df[keep_column_empre]])

#Merges and saves the data into a csv file
print("\nSaving file...")    
data=data_estab.merge(data_empre,on='CNPJ_BASICO',how='left')
data.to_csv("data_PR.csv",index=False)