from csv import reader
import mysql.connector
#from database.gerenciador_conexao_bd import connect
from database.gerenciador_conexao_bd import Connection
from util.dateUtil import converteData
from util.stringUtil import checarCampoVazio
from importersiconv.gerenciador_consultas import getIDConvenio, obtemIDConvenioTbTemp, obtemUltimaAtualizacao
from datetime import datetime

def atualizarProrrogaOficio(arquivo_csv_prorroga_oficio):
    db_connection = Connection.connect()

    numero_linhas_csv = 0
    numero_prorroga_oficio = 0
    
    # Obtém a Ultima_Atualizacao    
    ultima_atualizacao = obtemUltimaAtualizacao()
    ultima_atualizacao = ultima_atualizacao.date()
    
    with open(arquivo_csv_prorroga_oficio, 'r', encoding="utf8") as arquivo_csv:
        csv_reader = reader(arquivo_csv, delimiter=';')
        for linha in csv_reader:
            ##Leitura dos dados da planilha
            if numero_linhas_csv == 0:
                numero_linhas_csv = numero_linhas_csv + 1
                continue
            numero_linhas_csv = numero_linhas_csv + 1
            #Campos do CSV
            NR_CONVENIO = linha[0].strip()
            #Sem número do convênio
            if NR_CONVENIO == "":
                continue;
            ID_CONVENIO = getIDConvenio(NR_CONVENIO)
            #Convênio não encontrado
            if ID_CONVENIO == 0:
                continue;
            
            NR_PRORROGA = linha[1].strip()
            DT_INICIO_PRORROGA = converteData(linha[2].strip())
            DT_FIM_PRORROGA = converteData(linha[3].strip())
            DIAS_PRORROGA = checarCampoVazio(linha[4].strip())
            DT_ASSINATURA_PRORROGA = converteData(linha[5].strip())
            SIT_PRORROGA = linha[6].strip()
            
            if DT_ASSINATURA_PRORROGA == 'NULL':
                continue
            
            data = DT_ASSINATURA_PRORROGA.replace("'","").split('-')
            data_assinatura_prorrogacao = datetime(int(data[0]),int(data[1]),int(data[2])).date()
            
            if obtemIDConvenioTbTemp(ID_CONVENIO):
            
                sql = "UPDATE Prorroga_Oficio SET ID_Convenio = " + str(ID_CONVENIO) + ", Situacao_Prorroga = '" + str(SIT_PRORROGA) + \
                        "', Nr_Prorroga = '" + str(NR_PRORROGA) + "', Data_Inicio_Prorroga = " + str(DT_INICIO_PRORROGA) + \
                        ", Data_Fim_Prorroga = " + str(DT_FIM_PRORROGA) + ", Dias_Prorroga = " + str(DIAS_PRORROGA) + \
                        ", Data_Assinatura_Prorroga = " + str(DT_ASSINATURA_PRORROGA) + \
                        " WHERE ID_Convenio = " + str(ID_CONVENIO) + " AND Nr_Prorroga = '" + str(NR_PRORROGA) + "'"
            
            elif data_assinatura_prorrogacao > ultima_atualizacao:
            
                sql = "INSERT INTO Prorroga_Oficio(ID_Convenio, Situacao_Prorroga, Nr_Prorroga, Data_Inicio_Prorroga, \
                        Data_Fim_Prorroga, Dias_Prorroga, Data_Assinatura_Prorroga) VALUES(" + str(ID_CONVENIO) + ", '" + \
                        str(SIT_PRORROGA) + "', '" + str(NR_PRORROGA) + "', " + str(DT_INICIO_PRORROGA) + ", " + \
                        str(DT_FIM_PRORROGA) + ", " + str(DIAS_PRORROGA) + ", " + str(DT_ASSINATURA_PRORROGA) + ")"
            else:
                continue
            
            try:
                  #cursor = db_connection.cursor()
                db_connection = Connection.connect()
                cursor = Connection.getCursor()
                cursor.execute(sql)
                #cursor.close()
                db_connection.commit()
                numero_prorroga_oficio = numero_prorroga_oficio + 1
            except Exception as e:
                print("Erro ao gravar Prorrogação de Ofício %s" % (NR_PRORROGA))
                print(str(e))
                continue
            
    print("Gravadas %d Prorrogações de Ofício" % (numero_prorroga_oficio))