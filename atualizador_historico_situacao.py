#!/usr/bin/env python
# -*- coding: utf-8 -*-
from csv import reader
from datetime import datetime
import mysql.connector
# from database.gerenciador_conexao_bd import connect
from database.gerenciador_conexao_bd import Connection
from importersiconv.gerenciador_consultas import getIDProposta, getIDConvenio, propostaSDI 
from importersiconv.gerenciador_consultas import inserirIDPropostaTbTemp, inserirIDConvenioTbTemp, obtemUltimaAtualizacao
from util.dateUtil import converteDataHora
from util.stringUtil import checarCampoVazio

# Atualiza a tabela Historico_Situacao. Neste caso, são adicionados apenas os registros com
# Data_Historico_Situacao posterior a Ultima_Atualizacao (tabela Ultima_Atualizacao_Dados)
def atualizarHistoricoSituacao(arquivo_csv_historico_situacao):
    db_connection = Connection.connect()

    numero_linhas_csv = 0
    numero_historicos = 0

    # Obtém a Ultima_Atualizacao    
    dt2 = obtemUltimaAtualizacao()
        
    with open(arquivo_csv_historico_situacao, 'r') as arquivo_csv:
        csv_reader = reader(arquivo_csv, delimiter=';')
        for linha in csv_reader:
            
            ##Leitura dos dados da planilha
            if numero_linhas_csv == 0:
                numero_linhas_csv = numero_linhas_csv + 1
                continue
            numero_linhas_csv = numero_linhas_csv + 1
            
            # Data relativa ao histórico situação
            DIA_HISTORICO_SIT = converteDataHora(linha[2].strip())

            data_hora = DIA_HISTORICO_SIT.split(' ')
            data = (data_hora[0].replace("'", "")).split('-')
            hora = (data_hora[1].replace("'", "")).split(':')
            
            dt1 = datetime(int(data[0]),int(data[1]),int(data[2]),int(hora[0]),int(hora[1]),int(hora[2]))
            
            if dt1 > dt2:
                
                #Campos do CSV
                ID_PROPOSTA = linha[0].strip()
                ID_PROPOSTA = getIDProposta(ID_PROPOSTA)
                
                #Proposta não encontrada
                if ID_PROPOSTA == 0:
                    continue;
                                       
                inserirIDPropostaTbTemp(ID_PROPOSTA)
                                   
                NR_CONVENIO = linha[1].strip()
                ID_CONVENIO = getIDConvenio(NR_CONVENIO)
                if ID_CONVENIO == 0:
                    ID_CONVENIO = "NULL"
                
                # insere o convênio na tabela temporária, apenas se o ID_CONVENIO
                # for diferente de "NULL" 
                if ID_CONVENIO != "NULL":
                    inserirIDConvenioTbTemp(ID_CONVENIO)
            
                HISTORICO_SIT = linha[3].strip()
                DIAS_HISTORICO_SIT = checarCampoVazio(linha[4].strip())
                COD_HISTORICO_SIT = checarCampoVazio(linha[5].strip())

                sql = "INSERT INTO Historico_Situacao(ID_Proposta, ID_Convenio, Data_Historico_Situacao, Historico_Situacao, \
                        Dias_Historico_Situacao, Codigo_Historico_Situacao) VALUES(" + str(ID_PROPOSTA) + ", " + \
                        str(ID_CONVENIO) + ", " + str(DIA_HISTORICO_SIT) + ", '" + str(HISTORICO_SIT) + "', " + \
                        str(DIAS_HISTORICO_SIT) + ", " + str(COD_HISTORICO_SIT) + ")"
                               
                try:
                    #cursor = db_connection.cursor()
                    db_connection = Connection.connect()
                    cursor = Connection.getCursor()
                    cursor.execute(sql)
                    #cursor.close()
                    db_connection.commit()
                    numero_historicos = numero_historicos + 1
                except Exception as e:
                    print("Erro ao gravar Histórico de Situação da Proposta %s do dia %s" % (ID_PROPOSTA, DIA_HISTORICO_SIT))
                    print(str(e))
                    print(sql)
                    break
    
        print("Gravados %d Históricos de Situação" % (numero_historicos))