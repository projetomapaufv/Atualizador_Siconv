from csv import reader
import mysql.connector
from database.gerenciador_conexao_bd import Connection
from util.dateUtil import converteData
from util.stringUtil import checarCampoVazio, removeNonASCIICharacters
from importersiconv.gerenciador_consultas import getIDEmpenho, getIDDesembolso

def atualizarEmpenhosDesembolsos(arquivo_csv_empenho_desembolso):
    db_connection = Connection.connect()

    numero_linhas_csv = 0
    numero_empenhos_desembolsos = 0
    with open(arquivo_csv_empenho_desembolso, 'r') as arquivo_csv:
        csv_reader = reader(arquivo_csv, delimiter=';')
        for linha in csv_reader:
            ##Leitura dos dados da planilha
            if numero_linhas_csv == 0:
                numero_linhas_csv = numero_linhas_csv + 1
                continue
            numero_linhas_csv = numero_linhas_csv + 1
            #Campos do CSV
            ID_DESEMBOLSO = checarCampoVazio(linha[0].strip())
            if not ID_DESEMBOLSO == "NULL":
                ID_DESEMBOLSO = getIDDesembolso(ID_DESEMBOLSO)
            
            ID_EMPENHO = checarCampoVazio(linha[1].strip())
            if not ID_EMPENHO == "NULL":
                ID_EMPENHO = getIDEmpenho(ID_EMPENHO)
            
            #Não faz sentido salvar o registro se não há empenho ou desembolso
            if(ID_EMPENHO == "NULL" or ID_EMPENHO == 0) and (ID_DESEMBOLSO == "NULL" or ID_DESEMBOLSO == 0):
                continue;
            
            if (ID_EMPENHO == 0):
                ID_EMPENHO = "NULL"
            
            if(ID_DESEMBOLSO == 0):
                ID_DESEMBOLSO = "NULL"

            VALOR_GRUPO = checarCampoVazio(linha[2].strip().replace(",", "."))
            
            if getIDDesembolso(ID_DESEMBOLSO) or getIDEmpenho(ID_EMPENHO):

                sql = "INSERT INTO Empenho_Desembolso(ID_Empenho, ID_Desembolso, Valor_Grupo) VALUES (" + \
                        str(ID_EMPENHO) + ", " + str(ID_DESEMBOLSO) + ", " + str(VALOR_GRUPO) + ")"
            else:
                continue
            
            try:
                #cursor = db_connection.cursor()
                db_connection = Connection.connect()
                cursor = Connection.getCursor()
                cursor.execute(sql)
                #cursor.close()
                db_connection.commit()
                numero_empenhos_desembolsos = numero_empenhos_desembolsos + 1
            except Exception as e:
                print("Erro ao gravar Empenho_Desembolso  %s - %s" % (ID_EMPENHO, ID_DESEMBOLSO))
                print(str(e))
                print(sql)
                continue

    print("Gravados %d Empenhos_Desembolsos" % (numero_empenhos_desembolsos))  

