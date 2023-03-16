#!/usr/bin/env python
# -*- coding: utf-8 -*-
from csv import reader
#import mysql.connector
#from database.gerenciador_conexao_bd import connect
from database.gerenciador_conexao_bd import Connection
from datetime import date, datetime
from importersiconv.gerenciador_consultas import getIDProposta, getIDConvenio
from importersiconv.gerenciador_consultas import obtemUltimaAtualizacao, obtemIDConvenioTbTemp
from util.stringUtil import checarCampoVazio
from util.dateUtil import converteData

UG_SDI_MAPA = '420013'

#Grava os convênios do Mapa no banco de dados
def atualizarConvenios(arquivo_csv_convenios):
    db_connection = Connection.connect()

    numero_linhas_csv = 0
    numero_convenios = 0
    
    # Obtém a Ultima_Atualizacao    
    dt2 = obtemUltimaAtualizacao()
    dt2 = dt2.date()
    
    print(dt2)
    
    with open(arquivo_csv_convenios, 'r', encoding="utf8") as arquivo_csv:
        csv_reader = reader(arquivo_csv, delimiter=';')
        for linha in csv_reader:
            ##Leitura dos dados da planilha
            if numero_linhas_csv == 0:
                numero_linhas_csv = numero_linhas_csv + 1
                continue
            numero_linhas_csv = numero_linhas_csv + 1

            # data = (DIA_PUBL_CON.replace("'", "")).split('-')
        
            # dt1 = datetime(int(data[0]),int(data[1]),int(data[2]))
            # dt2 = datetime(2022,12,12)
                        
            # if dt1 > dt2:
            
            # UG_SDI_MAPA = '420013'
            UG_EMITENTE = linha[12].strip()
            if UG_EMITENTE != UG_SDI_MAPA:
                continue
            
            # Extrai o número do convênio
            NR_CONVENIO = linha[0].strip()
            
            ID_CONVENIO = getIDConvenio(NR_CONVENIO)
            
            #Código da Proposta no SICONV
            CODIGO_PROPOSTA = linha[1].strip()
            ID_PROPOSTA = getIDProposta(CODIGO_PROPOSTA)
            #Proposta não encontrada. Não salvo o Convênio
            if ID_PROPOSTA == 0:
                continue;

            DIA_ASSIN_CONV = converteData(linha[5].strip())
            SIT_CONVENIO = linha[6].strip()
            SUBSITUACAO_CONVENIO = linha[7].strip()
            SITUACAO_PUBLICACAO = linha[8].strip()
            
            INSTRUMENTO_ATIVO = linha[9].strip()
            if INSTRUMENTO_ATIVO.upper() == 'SIM':
                INSTRUMENTO_ATIVO = "TRUE"
            else:
                INSTRUMENTO_ATIVO = "FALSE"

            IND_OPERA_OBTV = linha[10].strip()
            if IND_OPERA_OBTV.upper() == 'SIM':
                IND_OPERA_OBTV = "TRUE"
            else:
                IND_OPERA_OBTV = "FALSE"

            NR_PROCESSO = linha[11].strip()
            
            # Extrai a data de publicação do convênio            
            DIA_PUBL_CON = converteData(linha[13].strip())

            if DIA_PUBL_CON == 'NULL':
                continue
        
            DIA_INICIO_VIGENCIA = converteData(linha[14].strip())
            DIA_FIM_VIGENC_CONV = converteData(linha[15].strip())
            DIA_FIM_VIGENC_ORIGINAL_CONV = converteData(linha[16].strip())
            DIAS_PREST_CONTAS = checarCampoVazio(linha[17].strip())
            DIA_LIMITE_PREST_CONTAS = converteData(linha[18].strip())
            DATA_SUSPENSIVA = converteData(linha[19].strip())
            DATA_RETIRADA_SUSPENSIVA = converteData(linha[20].strip())
            DIAS_CLAUSULA_SUSPENSIVA = checarCampoVazio(linha[21].strip())
            SITUACAO_CONTRATACAO = linha[22].strip()

            IND_ASSINADO = linha[23].strip()
            if IND_ASSINADO.upper() == 'SIM':
                IND_ASSINADO = "TRUE"
            else:
                IND_ASSINADO = "FALSE"

            MOTIVO_SUSPENSAO = linha[24].strip()

            IND_FOTO = linha[25].strip()
            if IND_FOTO.upper() == 'SIM':
                IND_FOTO = "TRUE"
            else:
                IND_FOTO = "FALSE"

            QTDE_CONVENIOS = checarCampoVazio(linha[26].strip())
            QTD_TA = checarCampoVazio(linha[27].strip())
            QTD_PRORROGA = checarCampoVazio(linha[28].strip())
            VL_GLOBAL_CONV = checarCampoVazio(linha[29].strip().replace(",", "."))
            VL_REPASSE_CONV = checarCampoVazio(linha[30].strip().replace(",", "."))
            VL_CONTRAPARTIDA_CONV = checarCampoVazio(linha[31].strip().replace(",", "."))
            VL_EMPENHADO_CONV = checarCampoVazio(linha[32].strip().replace(",", "."))
            VL_DESEMBOLSADO_CONV = checarCampoVazio(linha[33].strip().replace(",", "."))
            VL_SALDO_REMAN_TESOURO = checarCampoVazio(linha[34].strip().replace(",", "."))
            VL_SALDO_REMAN_CONVENENTE = checarCampoVazio(linha[35].strip().replace(",", "."))
            VL_RENDIMENTO_APLICACAO = checarCampoVazio(linha[36].strip().replace(",", "."))
            VL_INGRESSO_CONTRAPARTIDA = checarCampoVazio(linha[37].strip().replace(",", "."))
            VL_SALDO_CONTA = checarCampoVazio(linha[38].strip().replace(",", "."))
            VALOR_GLOBAL_ORIGINAL_CONV = checarCampoVazio(linha[39].strip().replace(",", "."))
            
            # Verifica se o convênio precisa ser atualizado         
            if ID_CONVENIO != 0 and obtemIDConvenioTbTemp(ID_CONVENIO): 
                
                sql = "UPDATE Convenio SET Nr_Convenio = " + str(NR_CONVENIO) + ", ID_Proposta = " + str(ID_PROPOSTA) +\
                      ", Situacao_Convenio = '" + str(SIT_CONVENIO) + "', Subsituacao_Convenio = '" + str(SUBSITUACAO_CONVENIO) +\
                      "', Situacao_Publicacao_Convenio = '" + str(SITUACAO_PUBLICACAO) + "', Situacao_Contratacao_Convenio = '" +\
                      str(SITUACAO_CONTRATACAO) + "', Data_Assinatura = "+ str(DIA_ASSIN_CONV) + ", Instrumento_Ativo = " +\
                      str(INSTRUMENTO_ATIVO) + ", Ind_Opera_OBTV = " + str(IND_OPERA_OBTV) + ", Nr_Processo = '" +\
                      str(NR_PROCESSO) + "', UG_Emitente = '" + str(UG_EMITENTE) + "', Data_Publicacao = " + str(DIA_PUBL_CON) +\
                      ", Data_Inicio_Vigencia = " + str(DIA_INICIO_VIGENCIA) + ", Data_Fim_Vigencia = " + str(DIA_INICIO_VIGENCIA) +\
                      ", Data_Fim_Vigencia_Original = " + str(DIA_FIM_VIGENC_ORIGINAL_CONV) + ", Dias_Prestacao_Contas = "+\
                      str(DIAS_PREST_CONTAS) + ", Data_Limite_Prestacao_Contas = " + str(DIA_LIMITE_PREST_CONTAS) + ", Data_Clausula_Suspensiva = " +\
                      str(DATA_SUSPENSIVA) + ", Data_Retirada_Clausula_Suspensiva = " + str(DATA_RETIRADA_SUSPENSIVA) +\
                      ", Dias_Clausula_Suspensiva = " + str(DIAS_CLAUSULA_SUSPENSIVA) + ", Ind_Assinado = " +\
                      str(IND_ASSINADO) + ", Motivo_Suspensao = '" + str(MOTIVO_SUSPENSAO) + "', Ind_Foto = " +\
                      str(IND_FOTO) + ", Qtde_Convenios = " + str(QTDE_CONVENIOS) + ", Qtde_TA = " + str(QTD_TA) + ", Qtde_Prorroga = " + str(QTD_PRORROGA) + ", Valor_Global_Convenio = " +\
                      str(VL_GLOBAL_CONV) + ", Valor_Repasse_Convenio = " + str(VL_REPASSE_CONV) + ", Valor_Contrapartida_Convenio = " + str(VL_CONTRAPARTIDA_CONV) + ", Valor_Empenhado_Convenio = " +\
                      str(VL_EMPENHADO_CONV) + ", Valor_Desembolsado_Convenio = " + str(VL_DESEMBOLSADO_CONV) + ", Valor_Saldo_Reman_Tesouro = " + str(VL_SALDO_REMAN_TESOURO) + ", Valor_Saldo_Reman_Convenente = " +\
                      str(VL_SALDO_REMAN_CONVENENTE) + ", Valor_Rendimento_Aplicacao = " + str(VL_RENDIMENTO_APLICACAO) + ", Valor_Ingresso_Contrapartida = " + str(VL_INGRESSO_CONTRAPARTIDA) + ", Valor_Saldo_Conta = " +\
                      str(VL_SALDO_CONTA) + ", Valor_Global_Original_Convenio = " + str(VALOR_GLOBAL_ORIGINAL_CONV) +\
                      " WHERE Nr_Convenio = " + str(NR_CONVENIO)
                                      
            # Verifica se o convênio deve ser inserido
            elif ID_CONVENIO == 0 and obtemIDConvenioTbTemp(ID_CONVENIO):
                
                sql = "INSERT INTO Convenio(Nr_Convenio, ID_Proposta, Situacao_Convenio, Subsituacao_Convenio, \
                        Situacao_Publicacao_Convenio, Situacao_Contratacao_Convenio, \
                        Data_Assinatura, Instrumento_Ativo, Ind_Opera_OBTV, Nr_Processo, UG_Emitente, Data_Publicacao, \
                        Data_Inicio_Vigencia, Data_Fim_Vigencia, Data_Fim_Vigencia_Original, Dias_Prestacao_Contas, \
                        Data_Limite_Prestacao_Contas, Data_Clausula_Suspensiva, Data_Retirada_Clausula_Suspensiva, \
                        Dias_Clausula_Suspensiva, Ind_Assinado, Motivo_Suspensao, Ind_Foto, Qtde_Convenios, Qtde_TA, \
                        Qtde_Prorroga, Valor_Global_Convenio, Valor_Repasse_Convenio, Valor_Contrapartida_Convenio, \
                        Valor_Empenhado_Convenio, Valor_Desembolsado_Convenio, Valor_Saldo_Reman_Tesouro, \
                        Valor_Saldo_Reman_Convenente, Valor_Rendimento_Aplicacao, Valor_Ingresso_Contrapartida, \
                        Valor_Saldo_Conta, Valor_Global_Original_Convenio) VALUES('" + str(NR_CONVENIO) + "', " + \
                        str(ID_PROPOSTA) + ", '" + str(SIT_CONVENIO) + "', '" + str(SUBSITUACAO_CONVENIO) + "', '" + \
                        str(SITUACAO_PUBLICACAO) + "', '" + str(SITUACAO_CONTRATACAO) + "', " + \
                        str(DIA_ASSIN_CONV) + ", " + str(INSTRUMENTO_ATIVO) + ", " + str(IND_OPERA_OBTV) + ", '" + \
                        str(NR_PROCESSO) + "', '" + str(UG_EMITENTE) + "', " + str(DIA_PUBL_CON) + ", " + \
                        str(DIA_INICIO_VIGENCIA) + ", " + str(DIA_FIM_VIGENC_CONV) + ", " + \
                        str(DIA_FIM_VIGENC_ORIGINAL_CONV) + ", " + str(DIAS_PREST_CONTAS) + ", " + \
                        str(DIA_LIMITE_PREST_CONTAS) + ", " + str(DATA_SUSPENSIVA) + ", " + str(DATA_RETIRADA_SUSPENSIVA) + ", " + \
                        str(DIAS_CLAUSULA_SUSPENSIVA) + ", " + str(IND_ASSINADO) + ", '" + str(MOTIVO_SUSPENSAO) + "', " + \
                        str(IND_FOTO) + ", " + str(QTDE_CONVENIOS) + ", " + str(QTD_TA) + ", " + str(QTD_PRORROGA) + ", " + \
                        str(VL_GLOBAL_CONV) + ", " + str(VL_REPASSE_CONV) + ", " + str(VL_CONTRAPARTIDA_CONV) + ", " + \
                        str(VL_EMPENHADO_CONV) + ", " + str(VL_DESEMBOLSADO_CONV) + ", " + str(VL_SALDO_REMAN_TESOURO) + ", " + \
                        str(VL_SALDO_REMAN_CONVENENTE) + ", " + str(VL_RENDIMENTO_APLICACAO) + ", " + str(VL_INGRESSO_CONTRAPARTIDA) + ", " + \
                        str(VL_SALDO_CONTA) + ", " + str(VALOR_GLOBAL_ORIGINAL_CONV) + ")"
            else:
                continue
            
            try:
                #cursor = db_connection.cursor()
                db_connection = Connection.connect()
                cursor = Connection.getCursor()
                cursor.execute(sql)
                #cursor.close()
                db_connection.commit()
                numero_convenios = numero_convenios + 1
            except Exception as e:
                print("Erro ao gravar Convênio %s" % NR_CONVENIO)
                print(str(e))
                continue
        
        print("Gravados %d Convênios" % (numero_convenios)) 

