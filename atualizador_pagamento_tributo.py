from csv import reader
import mysql.connector
#from database.gerenciador_conexao_bd import connect
from database.gerenciador_conexao_bd import Connection
from util.dateUtil import converteData
from util.stringUtil import checarCampoVazio
from importersiconv.gerenciador_consultas import getIDConvenio, obtemIDConvenioTbTemp, obtemUltimaAtualizacao
from datetime import datetime

def atualizarPagamentosTributos(arquivo_csv_pagamento_tributos):
    db_connection = Connection.connect()

    numero_linhas_csv = 0
    numero_pagamentos_tributos = 0
    
    # Obtém a Ultima_Atualizacao    
    ultima_atualizacao = obtemUltimaAtualizacao()
    ultima_atualizacao = ultima_atualizacao.date()
    
    with open(arquivo_csv_pagamento_tributos, 'r', encoding="utf8") as arquivo_csv:
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
            DATA_TRIBUTO = converteData(linha[1].strip())
            VL_PAGO_TRIBUTOS = checarCampoVazio(linha[2].strip().replace(",", "."))
            
            if DATA_TRIBUTO == 'NULL':
                continue
            
            data = DATA_TRIBUTO.replace("'","").split('-')
            data_tributo = datetime(int(data[0]),int(data[1]),int(data[2])).date()
            
            if obtemIDConvenioTbTemp(ID_CONVENIO):
            
                sql = "UPDATE Pagamento_Tributo SET ID_Convenio = " + str(ID_CONVENIO) + ", Data_Tributo = " + str(DATA_TRIBUTO) + \
                      ", Valor_Pago_Tributo = " + str(VL_PAGO_TRIBUTOS) + \
                      " WHERE ID_Convenio = " + str(ID_CONVENIO)
            
            elif data_tributo > ultima_atualizacao:
                
                sql = "INSERT INTO Pagamento_Tributo(ID_Convenio, Data_Tributo, Valor_Pago_Tributo) VALUES(" + \
                    str(ID_CONVENIO) + ", " + str(DATA_TRIBUTO) + ", " + str(VL_PAGO_TRIBUTOS) + ")"
            else:
                continue
                                   
            try:
                #cursor = db_connection.cursor()
                db_connection = Connection.connect()
                cursor = Connection.getCursor()
                cursor.execute(sql)
                #cursor.close()
                db_connection.commit()
                numero_pagamentos_tributos = numero_pagamentos_tributos + 1
            except Exception as e:
                print("Erro ao gravar Pagamento de Tributo de %s do Convênio %s" % (DATA_TRIBUTO, NR_CONVENIO))
                print(str(e))
                continue
    
    print("Gravadas %d Pagamentos de Tributos" % (numero_pagamentos_tributos))