#!/usr/bin/env python
# -*- coding: utf-8 -*-
from csv import reader
import mysql.connector
#from database.gerenciador_conexao_bd import connect
from database.gerenciador_conexao_bd import Connection

def getIDProponente(CNPJ_Proponente):
    #connection = Connection()
    db_connection = Connection.connect()

    sql = "SELECT ID_Proponente FROM Proponente WHERE CNPJ = {cnpj}".format(cnpj = CNPJ_Proponente)
    #cursor = db_connection.cursor(buffered=True)
    cursor = Connection.getBufferedCursor()
    cursor.execute(sql)

    proponente_encontrado = False
    for (ID_Proponente) in cursor:
        id_proponente = ID_Proponente
        proponente_encontrado = True

    #cursor.close()
    db_connection.commit()

    if proponente_encontrado:
        return id_proponente[0]
    else:
        return 0
    
def propostaSDI(codigo_proposta):
    #connection = Connection()
    db_connection = Connection.connect()

    sql = "SELECT Codigo_Proposta FROM Propostas_SDI_MAPA WHERE Codigo_Proposta = {cod_proposta}".format(cod_proposta = codigo_proposta)
    #cursor = db_connection.cursor(buffered=True)
    cursor = Connection.getBufferedCursor()
    cursor.execute(sql)

    proposta_encontrada = False
    for (ID_Proposta) in cursor:
        id_proposta = ID_Proposta
        proposta_encontrada = True

    #cursor.close()
    db_connection.commit()

    return proposta_encontrada

def getIDProposta(codigo_proposta_siconv):
    #connection = Connection()
    db_connection = Connection.connect()

    sql = "SELECT ID_Proposta FROM Proposta WHERE Codigo_Proposta = {cod_proposta_siconv}".format(cod_proposta_siconv = codigo_proposta_siconv)
    #cursor = db_connection.cursor(buffered=True)
    cursor = Connection.getBufferedCursor()
    cursor.execute(sql)

    proposta_encontrada = False
    for (ID_Proposta) in cursor:
        id_proposta = ID_Proposta
        proposta_encontrada = True
        break

    #cursor.close()
    db_connection.commit()

    if proposta_encontrada:
        return id_proposta[0]
    else:
        return 0
    
def getIDConvenio(numero_convenio):
    #connection = Connection()
    db_connection = Connection.connect()

    sql = "SELECT ID_Convenio FROM Convenio WHERE Nr_Convenio = {nr_convenio}".format(nr_convenio = numero_convenio)
    #cursor = db_connection.cursor(buffered=True)
    try:
        cursor = Connection.getBufferedCursor()
        cursor.execute(sql)

        convenio_encontrado = False
        for (ID_Convenio) in cursor:
            id_convenio = ID_Convenio
            convenio_encontrado = True
            break

        #cursor.close()
        db_connection.commit()
    except:
        print("Erro ao consultar Convênio")
        print(sql)
        return 0

    if convenio_encontrado:
        return id_convenio[0]
    else:
        return 0

def getIDMetaCronoFisica(codigo_meta_siconv):
    #connection = Connection()
    db_connection = Connection.connect()

    sql = "SELECT ID_Meta_Crono_Fisico FROM Meta_Crono_Fisico WHERE Codigo_Meta_Crono_Fisico = {cd_meta_siconv}".format(cd_meta_siconv = codigo_meta_siconv)
    #cursor = db_connection.cursor(buffered=True)
    cursor = Connection.getBufferedCursor()
    cursor.execute(sql)

    meta_encontrada = False
    for (ID_Meta) in cursor:
        id_meta = ID_Meta
        meta_encontrada = True
        break

    #cursor.close()
    db_connection.commit()

    if meta_encontrada:
        return id_meta[0]
    else:
        return 0
    
def getIDEtapaCronoFisica(codigo_etapa):
    #connection = Connection()
    db_connection = Connection.connect()

    sql = "SELECT ID_Etapa_Crono_Fisico FROM Etapa_Crono_Fisico WHERE Codigo_Etapa = {cd_etapa}".format(cd_etapa = codigo_etapa)
    #cursor = db_connection.cursor(buffered=True)
    cursor = Connection.getBufferedCursor()
    cursor.execute(sql)

    etapa_encontrada = False
    for (ID_Etapa) in cursor:
        id_etapa = ID_Etapa
        etapa_encontrada = True
        break

    #cursor.close()
    db_connection.commit()

    if etapa_encontrada:
        return id_etapa[0]
    else:
        return 0

def getIDEmpenho(codigo_empenho_siconv):
    db_connection = Connection.connect()

    sql = "SELECT ID_Empenho FROM Empenho WHERE Codigo_Empenho_Siconv = {cd_empenho_siconv}".format(cd_empenho_siconv = codigo_empenho_siconv)
    #cursor = db_connection.cursor(buffered=True)
    cursor = Connection.getBufferedCursor()
    cursor.execute(sql)

    empenho_encontrado = False
    for (ID_Empenho) in cursor:
        id_empenho = ID_Empenho
        empenho_encontrado = True
        break

    #cursor.close()
    db_connection.commit()

    if empenho_encontrado:
        return id_empenho[0]
    else:
        return 0

def getIDDesembolso(codigo_desembolso_siconv):
    db_connection = Connection.connect()

    sql = "SELECT ID_Desembolso FROM Desembolso WHERE Codigo_Desembolso_Siconv = {cd_desembolso_siconv}".format(cd_desembolso_siconv = codigo_desembolso_siconv)
    #cursor = db_connection.cursor(buffered=True)
    cursor = Connection.getBufferedCursor()
    cursor.execute(sql)

    desembolso_encontrado = False
    for (ID_Desembolso) in cursor:
        id_desembolso = ID_Desembolso
        desembolso_encontrado = True
        break

    #cursor.close()
    db_connection.commit()

    if desembolso_encontrado:
        return id_desembolso[0]
    else:
        return 0
    
def getIDLicitacao(codigo_licitacao_siconv):
    db_connection = Connection.connect()

    sql = "SELECT ID_Licitacao FROM Licitacao WHERE Codigo_Licitacao_Siconv = {cd_licitacao_siconv}".format(cd_licitacao_siconv = codigo_licitacao_siconv)
    #cursor = db_connection.cursor(buffered=True)
    cursor = Connection.getBufferedCursor()
    cursor.execute(sql)

    licitacao_encontrada = False
    for (ID_Licitacao) in cursor:
        id_licitacao = ID_Licitacao
        licitacao_encontrada = True
        break

    #cursor.close()
    db_connection.commit()

    if licitacao_encontrada:
        return id_licitacao[0]
    else:
        return 0

def getIDPagamento(nr_movimentacao_financeira):
    db_connection = Connection.connect()

    sql = "SELECT ID_Pagamento FROM Pagamento WHERE Nr_Movimentacao_Financeira = {nr_mov_fin}".format(nr_mov_fin = nr_movimentacao_financeira)
    #cursor = db_connection.cursor(buffered=True)
    cursor = Connection.getBufferedCursor()
    cursor.execute(sql)

    pagamento_encontrado = False
    for (ID_Pagamento) in cursor:
        id_pagamento = ID_Pagamento
        pagamento_encontrado = True
        break

    #cursor.close()
    db_connection.commit()

    if pagamento_encontrado:
        return id_pagamento[0]
    else:
        return 0

# Insere em uma tabela temporária o ID_Proposta de registros do arquivo siconv_historico_situacao.csv, 
# para os quais a Data_Historico_Situacao seja posterior a Ultima_Atualizacao (tabela Ultima_Atualizacao_Dados). 
def inserirIDPropostaTbTemp(id_proposta):
    
    sql = "INSERT INTO Temp_Proposta (ID_Proposta) VALUES ("+str(id_proposta)+")"
    
    try:
        db_connection = Connection.connect()
        cursor = Connection.getCursor()
        cursor.execute(sql)
        db_connection.commit()
    except Exception as e:
        print(str(e))
        print(sql)
        
# Insere em uma tabela temporária o ID_Convenio de registros do arquivo siconv_historico_situacao.csv, 
# para os quais a Data_Historico_Situacao seja posterior a Ultima_Atualizacao (tabela Ultima_Atualizacao_Dados). 
def inserirIDConvenioTbTemp(id_convenio):
    
    sql = "INSERT INTO Temp_Convenio (ID_Convenio) VALUES ("+str(id_convenio)+")"
    
    try:
        db_connection = Connection.connect()
        cursor = Connection.getCursor()
        cursor.execute(sql)
        db_connection.commit()
    except Exception as e:
        print(str(e))
        print(sql)
        
        
# Seleciona o Id_Convenio (int) registrado na tabela temporária de convênios
def obtemIDConvenioTbTemp(Id_Convenio):
    
    sql = "SELECT * FROM Temp_Convenio WHERE ID_Convenio = "+str(Id_Convenio)
    
    try:
        db_connection = Connection.connect()
        cursor = Connection.getBufferedCursor()
        cursor.execute(sql)
        db_connection.commit()
        
        for row in cursor:
            if row[0] == int(Id_Convenio):
                Id_Convenio = row[0]
                return True
            else:
                return False
            
    except Exception as e:
        print(str(e))
        print(sql)

# Seleciona o Codigo_Proposta (int) registrado na tabela temporária de proposta
def obtemIDPropostaTbTemp(Id_Proposta):
    
    sql = "SELECT * FROM Temp_Proposta WHERE ID_Proposta = "+str(Id_Proposta)
    
    try:
        db_connection = Connection.connect()
        cursor = Connection.getBufferedCursor()
        cursor.execute(sql)
        db_connection.commit()
        
        for row in cursor:
            if row[0] == int(Id_Proposta):
                Cod_Proposta = row[0]
                return True
            else:
                return False
            
    except Exception as e:
        print(str(e))
        print(sql)

# Seleciona a Ultima_Atualizacao (datetime) registrada na tabela Ultima_Atualizacao_Dados
def obtemUltimaAtualizacao():
    
    sql = "SELECT MAX(Ultima_Atualizacao) FROM Ultima_Atualizacao_Dados"
    
    try:
        db_connection = Connection.connect()
        cursor = Connection.getBufferedCursor()
        cursor.execute(sql)
        db_connection.commit()
        
        for row in cursor:
            Ultima_Atualizacao = row[0] 
        return Ultima_Atualizacao
        
    except Exception as e:
        print(str(e))
        print(sql)
