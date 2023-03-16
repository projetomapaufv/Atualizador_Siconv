from csv import reader
import mysql.connector
from database.gerenciador_conexao_bd import Connection
from datetime import date, datetime
from util.dateUtil import converteData
from util.stringUtil import checarCampoVazio, removeNonASCIICharacters
from importersiconv.gerenciador_consultas import getIDConvenio, getIDDesembolso
from importersiconv.gerenciador_consultas import obtemUltimaAtualizacao, obtemIDConvenioTbTemp

def atualizarDesembolsos(arquivo_csv_desembolso):
    db_connection = Connection.connect()

    numero_linhas_csv = 0
    numero_desembolsos = 0
    
    # Obtém a Ultima_Atualizacao    
    dt2 = obtemUltimaAtualizacao()
    dt2 = dt2.date()    
    
    with open(arquivo_csv_desembolso, 'r', encoding="utf8") as arquivo_csv:
        csv_reader = reader(arquivo_csv, delimiter=';')
        for linha in csv_reader:
            # Leitura dos dados da planilha
            if numero_linhas_csv == 0:
                numero_linhas_csv = numero_linhas_csv + 1
                continue
            numero_linhas_csv = numero_linhas_csv + 1
            
            # Extrai a data de desembolso
            DATA_DESEMBOLSO = converteData(linha[4].strip())
            
            if DATA_DESEMBOLSO == 'NULL':
                continue

            data = (DATA_DESEMBOLSO.replace("'", "")).split('-')
                   
            dt1 = datetime(int(data[0]),int(data[1]),int(data[2])).date()

            # print(dt1)            
            
            # #Campos do CSV
            ID_DESEMBOLSO = checarCampoVazio(linha[0].strip())
            NR_CONVENIO = linha[1].strip()
            
            # Id do convênio                       
            ID_CONVENIO = getIDConvenio(NR_CONVENIO)
            #Convênio não encontrado
            if ID_CONVENIO == 0:
                continue;
            
            DT_ULT_DESEMBOLSO = converteData(linha[2].strip())
            QTD_DIAS_SEM_DESEMBOLSO = checarCampoVazio(linha[3].strip())
            
            # ANO_DESEMBOLSO = checarCampoVazio(linha[5].strip())
            # MES_DESEMBOLSO = checarCampoVazio(linha[6].strip())
            NR_SIAFI = linha[7].strip()
            VL_DESEMBOLSADO = checarCampoVazio(linha[8].strip().replace(",", "."))
                
            if obtemIDConvenioTbTemp(ID_CONVENIO) and getIDDesembolso(ID_DESEMBOLSO):
                
                sql = "UPDATE Desembolso SET ID_Convenio = "+ str(ID_CONVENIO) +", "+"Codigo_Desembolso_Siconv = " + str(ID_DESEMBOLSO) + ", " + \
                      "Data_Ultimo_Desembolso = " + str(DT_ULT_DESEMBOLSO) + ", " + \
                      "Qtde_Dias_Sem_Desembolso = " + str(QTD_DIAS_SEM_DESEMBOLSO) + ", " + \
                      "Data_Desembolso = " + str(DATA_DESEMBOLSO) + ", " + \
                      "Nr_SIAFI = '" + str(NR_SIAFI) + "', " + \
                      "Valor_Desembolsado = " + str(VL_DESEMBOLSADO) + \
                      " WHERE Codigo_Desembolso_Siconv = " + str(ID_DESEMBOLSO)
                      
            elif obtemIDConvenioTbTemp(ID_CONVENIO) and getIDDesembolso(ID_DESEMBOLSO)==0:
                
                sql = "INSERT INTO Desembolso(ID_Convenio, Codigo_Desembolso_Siconv, Data_Ultimo_Desembolso, Qtde_Dias_Sem_Desembolso,\
                        Data_Desembolso, Nr_SIAFI, Valor_Desembolsado) VALUES(" + str(ID_CONVENIO) + ", " + str(ID_DESEMBOLSO) + ", " + \
                        str(DT_ULT_DESEMBOLSO) + ", " + str(QTD_DIAS_SEM_DESEMBOLSO) + ", " + str(DATA_DESEMBOLSO) + ", '" + str(NR_SIAFI) + "', " + \
                        str(VL_DESEMBOLSADO) + ")"
            else:
                continue
              
            try:
                #cursor = db_connection.cursor()
                db_connection = Connection.connect()
                cursor = Connection.getCursor()
                cursor.execute(sql)
                #cursor.close()
                db_connection.commit()
                numero_desembolsos = numero_desembolsos + 1
            except Exception as e:
                print("Erro ao gravar Desembolso de %s do Convênio %s" % (VL_DESEMBOLSADO, NR_CONVENIO))
                print(str(e))
                print(sql)
                continue
            
        print("Gravadas %d Desembolsos" % (numero_desembolsos)) 
 