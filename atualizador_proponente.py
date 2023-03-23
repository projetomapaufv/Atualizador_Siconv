#!/usr/bin/env python
# -*- coding: utf-8 -*-
from csv import reader
import mysql.connector
#from database.gerenciador_conexao_bd import connect
from database.gerenciador_conexao_bd import Connection
from importersiconv.gerenciador_consultas import getIDProponente
import re

def atualizarProponentes(arquico_csv_proponentes):
    db_connection = Connection.connect()

    numero_linhas_csv = 0
    with open(arquico_csv_proponentes, 'r', encoding="utf8") as arquivo_csv:
        csv_reader = reader(arquivo_csv, delimiter=';')
        for linha in csv_reader:
            ##Leitura dos dados da planilha
            if numero_linhas_csv == 0:
                numero_linhas_csv = numero_linhas_csv + 1
                continue
            numero_linhas_csv = numero_linhas_csv + 1
            #Campos do CSV
            IDENTIF_PROPONENTE = linha[0].strip()
            NM_PROPONENTE = linha[1].strip()
            MUNICIPIO_PROPONENTE = linha[2].strip()
            UF_PROPONENTE = linha[3].strip()
            ENDERECO_PROPONENTE = linha[4].strip()
            BAIRRO_PROPONENTE = linha[5].strip()
            CEP_PROPONENTE = linha[6].strip()
            EMAIL_PROPONENTE = linha[7].strip()
            TELEFONE_PROPONENTE = linha[8].strip()
            FAX_PROPONENTE = linha[9].strip()
            
            if not IDENTIF_PROPONENTE.isdigit():
                continue
                       
            if getIDProponente(IDENTIF_PROPONENTE)==0:

                sql = "INSERT INTO Proponente(CNPJ, Nome, Municipio, UF, Endereco, Bairro, CEP, Email, Telefone, Fax) \
                        VALUES(" + str(IDENTIF_PROPONENTE) + ", '" + str(NM_PROPONENTE) + "', '" + str(MUNICIPIO_PROPONENTE) + "', '" + str(UF_PROPONENTE) + \
                                "', '" + str(ENDERECO_PROPONENTE) + "', '" + str(BAIRRO_PROPONENTE) + "', '" + str(CEP_PROPONENTE) + \
                                "', '" + str(EMAIL_PROPONENTE) + "', '" + str(TELEFONE_PROPONENTE) + \
                                "', '" + str(FAX_PROPONENTE) + "')"               
            else:
                continue
            
            try:
                  #cursor = db_connection.cursor()
                db_connection = Connection.connect()
                cursor = Connection.getCursor()
                cursor.execute(sql)
                #cursor.close()
                db_connection.commit()
            except Exception as e:
                print("Erro ao gravar Proponente %s" % IDENTIF_PROPONENTE)
                print(str(e))
                print(sql)
                continue
    
    print("Gravados %d Proponentes" % (numero_linhas_csv - 1))