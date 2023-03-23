from csv import reader
import mysql.connector
from database.gerenciador_conexao_bd import Connection
from util.dateUtil import converteDataHora
from util.stringUtil import checarCampoVazio, removeNonASCIICharacters
from importersiconv.gerenciador_consultas import getIDConvenio, getIDProposta, obtemIDPropostaTbTemp

def atualizarCronogramaDesembolso(arquivo_csv_cronograma_desembolso):
    #db_connection = Connection.connect()

    numero_linhas_csv = 0
    numero_cronogramas_desembolso = 0
    with open(arquivo_csv_cronograma_desembolso, 'r') as arquivo_csv:
        csv_reader = reader(arquivo_csv, delimiter=';')
        for linha in csv_reader:
            ##Leitura dos dados da planilha
            if numero_linhas_csv == 0:
                numero_linhas_csv = numero_linhas_csv + 1
                continue
            numero_linhas_csv = numero_linhas_csv + 1
            #Campos do CSV
            ID_PROPOSTA = linha[0].strip()
            ID_PROPOSTA = getIDProposta(ID_PROPOSTA)
            #Proposta não encontrada
            if ID_PROPOSTA == 0:
                continue;
            NR_CONVENIO = linha[1].strip()
            ID_CONVENIO = "NULL";
            if NR_CONVENIO != '':
                ID_CONVENIO = getIDConvenio(NR_CONVENIO)
            NR_PARCELA_CRONO_DESEMBOLSO = checarCampoVazio(linha[2].strip())
            MES_CRONO_DESEMBOLSO = checarCampoVazio(linha[3].strip())
            ANO_CRONO_DESEMBOLSO = checarCampoVazio(linha[4].strip())
            TIPO_RESP_CRONO_DESEMBOLSO = (linha[5].strip().replace('\\',''))
            VALOR_PARCELA_CRONO_DESEMBOLSO = checarCampoVazio(linha[6].strip().replace(",", "."))
            
            if obtemIDPropostaTbTemp(ID_PROPOSTA):
                                
                sql = "UPDATE Cronograma_Desembolso SET ID_Proposta = " + str(ID_PROPOSTA) + ", ID_Convenio = " + str(ID_CONVENIO) + \
                        ", Tipo_Responsavel_Cronograma_Desembolso = '" + str(TIPO_RESP_CRONO_DESEMBOLSO) + "', Nr_Parcela_Cronograma_Desembolso = " + str(NR_PARCELA_CRONO_DESEMBOLSO) + \
                        ", Mes_Cronograma_Desembolso = " + str(MES_CRONO_DESEMBOLSO) + ", Ano_Cronograma_Desembolso = " + str(ANO_CRONO_DESEMBOLSO) + \
                        ", Valor_Parcela_Desembolso = " + str(VALOR_PARCELA_CRONO_DESEMBOLSO) + \
                        " WHERE ID_Proposta = " + str(ID_PROPOSTA) + " AND Nr_Parcela_Cronograma_Desembolso = " + str(NR_PARCELA_CRONO_DESEMBOLSO)            
            else:
                continue
                        
            try:
                #cursor = db_connection.cursor()
                db_connection = Connection.connect()
                cursor = Connection.getCursor()
                cursor.execute(sql)
                #cursor.close()
                db_connection.commit()
                numero_cronogramas_desembolso = numero_cronogramas_desembolso + 1
            except Exception as e:
                print("Erro ao gravar Cronograma de Desembolso de parcela %s da Proposta %s" % (NR_PARCELA_CRONO_DESEMBOLSO, ID_PROPOSTA))
                print(str(e))
                print(sql)
                continue
    
    print("Gravadas %d Cronogramas de Desembolso" % (numero_cronogramas_desembolso)) 