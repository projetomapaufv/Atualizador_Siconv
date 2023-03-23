from csv import reader
import mysql.connector
#from database.gerenciador_conexao_bd import connect
from database.gerenciador_conexao_bd import Connection
from util.dateUtil import converteData
from util.stringUtil import checarCampoVazio, removeNonASCIICharacters
from importersiconv.gerenciador_consultas import getIDConvenio, obtemIDConvenioTbTemp, obtemUltimaAtualizacao
from datetime import datetime

def atualizarTermosAditivos(arquivo_csv_termo_aditivo):
    db_connection = Connection.connect()

    numero_linhas_csv = 0
    numero_termos_aditivos = 0
    
    # Obtém a Ultima_Atualizacao    
    ultima_atualizacao = obtemUltimaAtualizacao()
    ultima_atualizacao = ultima_atualizacao.date()
    
    with open(arquivo_csv_termo_aditivo, 'r', encoding="utf8") as arquivo_csv:
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
            NUMERO_TA = linha[2].strip()
            TIPO_TA = linha[3].strip()
            
            VL_GLOBAL_TA = checarCampoVazio(linha[4].strip().replace(",", "."))
            VL_REPASSE_TA = checarCampoVazio(linha[5].strip().replace(",", "."))
                        
            VL_CONTRAPARTIDA_TA = checarCampoVazio(linha[6].strip().replace(",", "."))
            DT_ASSINATURA_TA = converteData(linha[7].strip())
            DT_INICIO_TA = converteData(linha[8].strip())
            DT_FIM_TA = converteData(linha[9].strip())
            JUSTIFICATIVA_TA = removeNonASCIICharacters(linha[10].strip().replace("'", "\\'"))
            
            if DT_ASSINATURA_TA == 'NULL':
                continue
            
            data = DT_ASSINATURA_TA.replace("'","").split('-')
            data_termo_aditivo = datetime(int(data[0]),int(data[1]),int(data[2])).date()
            
            if obtemIDConvenioTbTemp(ID_CONVENIO):
                
                sql = "UPDATE Termo_Aditivo SET ID_Convenio = " + str(ID_CONVENIO) + ", Numero_TA = '" + str(NUMERO_TA) + "', Tipo_TA = '" + str(TIPO_TA) + \
                        "', Valor_Global_TA = " +  str(VL_GLOBAL_TA) + ", Valor_Repasse_TA = " + str(VL_REPASSE_TA) + ", Valor_Contrapartida_TA = " + \
                        str(VL_CONTRAPARTIDA_TA) + ", Data_Assinatura_TA = " + str(DT_ASSINATURA_TA) + ", Data_Inicio_TA = " + \
                        str(DT_INICIO_TA) + ", Data_Fim_TA = " + str(DT_FIM_TA) + ", Justificativa_TA = '"  + str(JUSTIFICATIVA_TA) + \
                        "' WHERE ID_Convenio = " + str(ID_CONVENIO) + " AND Numero_TA = '" + str(NUMERO_TA) + "'"
            
            elif data_termo_aditivo > ultima_atualizacao:
                
                sql = "INSERT INTO Termo_Aditivo(ID_Convenio, Numero_TA, Tipo_TA, Valor_Global_TA, Valor_Repasse_TA, \
                        Valor_Contrapartida_TA, Data_Assinatura_TA, Data_Inicio_TA, Data_Fim_TA, Justificativa_TA) VALUES (" + \
                        str(ID_CONVENIO) + ", '" + str(NUMERO_TA) + "', '" + str(TIPO_TA) + "', " +  str(VL_GLOBAL_TA) + ", " + \
                        str(VL_REPASSE_TA) + ", " + str(VL_CONTRAPARTIDA_TA) + ", " + str(DT_ASSINATURA_TA) + ", " + \
                        str(DT_INICIO_TA) + ", " + str(DT_FIM_TA) + ", '" + str(JUSTIFICATIVA_TA) + "')"
            else:
                continue
                                                          
            try:
                #cursor = db_connection.cursor()
                db_connection = Connection.connect()
                cursor = Connection.getCursor()
                cursor.execute(sql)
                #cursor.close()
                db_connection.commit()
                numero_termos_aditivos = numero_termos_aditivos + 1
            except Exception as e:
                print("Erro ao gravar Termo Aditivo %s do Convênio %s" % (NUMERO_TA, NR_CONVENIO))
                print(str(e))
                continue
            
    print("Gravadas %d Termos Aditivos" % (numero_termos_aditivos))