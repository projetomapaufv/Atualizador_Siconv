from csv import reader
import mysql.connector
#from database.gerenciador_conexao_bd import connect
from database.gerenciador_conexao_bd import Connection
from util.dateUtil import converteData
from util.stringUtil import checarCampoVazio
from importersiconv.gerenciador_consultas import getIDConvenio, getIDLicitacao
from importersiconv.gerenciador_consultas import obtemUltimaAtualizacao, obtemIDConvenioTbTemp
from datetime import datetime

def atualizarLicitacoes(arquivo_csv_licitacoes):
    db_connection = Connection.connect()

    numero_linhas_csv = 0
    numero_licitacoes = 0
    
    # Obtém a Ultima_Atualizacao    
    ultima_atualizacao = obtemUltimaAtualizacao()
    ultima_atualizacao = ultima_atualizacao.date()
    
    with open(arquivo_csv_licitacoes, 'r', encoding="utf8") as arquivo_csv:
        csv_reader = reader(arquivo_csv, delimiter=';')
        for linha in csv_reader:
            ##Leitura dos dados da planilha
            if numero_linhas_csv == 0:
                numero_linhas_csv = numero_linhas_csv + 1
                continue
            numero_linhas_csv = numero_linhas_csv + 1
            #Campos do CSV
            ID_LICITACAO = linha[0].strip()
            NR_CONVENIO = linha[1].strip()
            #Sem número do convênio
            if NR_CONVENIO == "":
                continue;
            ID_CONVENIO = getIDConvenio(NR_CONVENIO)
            #Convênio não encontrado
            if ID_CONVENIO == 0:
                continue;
            NR_LICITACAO = linha[2].strip()
            MODALIDADE_LICITACAO = linha[3].strip()
            TP_PROCESSO_COMPRA = linha[4].strip()
            TIPO_LICITACAO = linha[5].strip()
            NR_PROCESSO_LICITACAO = linha[6].strip()
            DATA_PUBLICACAO_LICITACAO = converteData(linha[7].strip())
            DATA_ABERTURA_LICITACAO = converteData(linha[8].strip())
            
            if DATA_ABERTURA_LICITACAO == 'NULL':
                continue
            
            data = DATA_ABERTURA_LICITACAO.replace("'","").split('-')
            data_licitacao = datetime(int(data[0]),int(data[1]),int(data[2])).date()
            
            DATA_ENCERRAMENTO_LICITACAO = converteData(linha[9].strip())
            DATA_HOMOLOGACAO_LICITACAO = converteData(linha[10].strip())
            STATUS_LICITACAO = linha[11].strip()
            SITUACAO_ACEITE_PROCESSO_EXECU = linha[12].strip()
            SISTEMA_ORIGEM = linha[13].strip()
            SITUACAO_SISTEMA = linha[14].strip()
            VALOR_LICITACAO = checarCampoVazio(linha[15].strip().replace(",", "."))
            
            if obtemIDConvenioTbTemp(ID_CONVENIO) and getIDLicitacao(ID_LICITACAO):
                
                sql = "UPDATE Licitacao SET ID_Convenio = " + str(ID_CONVENIO) + ", Modalidade_Licitacao = '" + str(MODALIDADE_LICITACAO) + "', Tipo_Processo_Compra = '" +  \
                        str(TP_PROCESSO_COMPRA)  + "', Tipo_Licitacao = '" + str(TIPO_LICITACAO) + "', Status_Licitacao = '" + str(STATUS_LICITACAO) + \
                        "', Codigo_Licitacao_Siconv = " + str(ID_LICITACAO) + ", Nr_Licitacao = '" + str(NR_LICITACAO) + "', Nr_Processo_Licitacao = '" + \
                        "', Data_Publicacao_Licitacao = " + str(DATA_PUBLICACAO_LICITACAO) + ", Data_Abertura_Licitacao = " + str(DATA_ABERTURA_LICITACAO) + \
                        ", Data_Encerramento_Licitacao = " + str(DATA_ENCERRAMENTO_LICITACAO) + ", Data_Homologacao_Licitacao = " + str(DATA_HOMOLOGACAO_LICITACAO) + \
                        ", Situacao_Aceite_Processo_Execucao = '" + str(SITUACAO_ACEITE_PROCESSO_EXECU) + "', Sistema_Origem = '" + str(SISTEMA_ORIGEM) + \
                        "', Situacao_Sistema = '" + str(SITUACAO_SISTEMA) + "', Valor_Licitacao = " + str(VALOR_LICITACAO) + \
                        " WHERE Codigo_Licitacao_Siconv = " + str(ID_LICITACAO)
                        
            elif data_licitacao > ultima_atualizacao and getIDLicitacao(ID_LICITACAO)==0:
                
                sql = "INSERT INTO Licitacao(ID_Convenio, Modalidade_Licitacao, Tipo_Processo_Compra, \
                        Tipo_Licitacao, Status_Licitacao, Codigo_Licitacao_Siconv, Nr_Licitacao, Nr_Processo_Licitacao, \
                        Data_Publicacao_Licitacao, Data_Abertura_Licitacao, Data_Encerramento_Licitacao, \
                        Data_Homologacao_Licitacao, Situacao_Aceite_Processo_Execucao, Sistema_Origem, \
                        Situacao_Sistema, Valor_Licitacao) VALUES(" + str(ID_CONVENIO) + ", '" + str(MODALIDADE_LICITACAO) + "', '" + \
                        str(TP_PROCESSO_COMPRA)  + "', '" + str(TIPO_LICITACAO) + "', '" + str(STATUS_LICITACAO) + "', " + \
                        str(ID_LICITACAO) + ", '" + str(NR_LICITACAO) + "', '" + str(NR_PROCESSO_LICITACAO) + "', " + \
                        str(DATA_PUBLICACAO_LICITACAO) + ", " + str(DATA_ABERTURA_LICITACAO) + ", " + str(DATA_ENCERRAMENTO_LICITACAO) + ", " + \
                        str(DATA_HOMOLOGACAO_LICITACAO) + ", '" + str(SITUACAO_ACEITE_PROCESSO_EXECU) + "', '" + str(SISTEMA_ORIGEM) + "', '" + \
                        str(SITUACAO_SISTEMA) + "', " + str(VALOR_LICITACAO) + ")"
            else:
                continue
                                               
            try:
                  #cursor = db_connection.cursor()
                db_connection = Connection.connect()
                cursor = Connection.getCursor()
                cursor.execute(sql)
                #cursor.close()
                db_connection.commit()
                numero_licitacoes = numero_licitacoes + 1
            except Exception as e:
                print("Erro ao gravar Licitação %s" % ID_LICITACAO)
                print(str(e))
                continue
        
    print("Gravadas %d Licitações" % (numero_licitacoes)) 

        
