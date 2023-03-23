from csv import reader
import mysql.connector
#from database.gerenciador_conexao_bd import connect
from database.gerenciador_conexao_bd import Connection
from util.dateUtil import converteData
from util.stringUtil import checarCampoVazio, removeNonASCIICharacters
from importersiconv.gerenciador_consultas import getIDMetaCronoFisica, getIDEtapaCronoFisica

def atualizarEtapasCronoFisicas(arquivo_csv_etapa_crono_fisica):
    #connection = Connection()
    db_connection = Connection.connect()

    numero_linhas_csv = 0
    numero_etapas_crono = 0
    with open(arquivo_csv_etapa_crono_fisica, 'r', encoding="utf8") as arquivo_csv:
        csv_reader = reader(arquivo_csv, delimiter=';')
        for linha in csv_reader:
            ##Leitura dos dados da planilha
            if numero_linhas_csv == 0:
                numero_linhas_csv = numero_linhas_csv + 1
                continue
            numero_linhas_csv = numero_linhas_csv + 1
            #Campos do CSV
            ID_META = linha[0].strip()
            #Sem ID de Meta. Não salvo
            if ID_META == "":
                continue
            ID_META = getIDMetaCronoFisica(ID_META)
            #Meta não encontrada, não salvo
            if ID_META == 0:
                continue
            ID_ETAPA = linha[1].strip()
            NR_ETAPA = linha[2].strip()
            DESC_ETAPA = removeNonASCIICharacters(linha[3].strip().replace("'", "\\'"))
            DATA_INICIO_ETAPA = converteData(linha[4].strip())
            DATA_FIM_ETAPA = converteData(linha[5].strip())
            UF_ETAPA = linha[6].strip()
            MUNICIPIO_ETAPA = (removeNonASCIICharacters(linha[7].strip().replace("'", "\\'"))).replace('\\','')
            ENDERECO_ETAPA = (removeNonASCIICharacters(linha[8].strip().replace("'", "\\'"))).replace('\\','')
            CEP_ETAPA = linha[9].strip()
            QTD_ETAPA = linha[10].strip()
            UND_FORNECIMENTO_ETAPA = linha[11].strip()
            VL_ETAPA = checarCampoVazio(linha[12].strip().replace(",", "."))
                        
            if getIDEtapaCronoFisica(ID_ETAPA):
                
                sql = "UPDATE Etapa_Crono_Fisico SET ID_Meta_Crono_Fisico = " + str(ID_META) +  ", Codigo_Etapa = " + str(ID_ETAPA) + \
                      ", Nr_Etapa = " + str(NR_ETAPA) + ", Descricao_Etapa = '" + str(DESC_ETAPA) + "', Data_Inicio_Etapa = " + str(DATA_INICIO_ETAPA) + \
                      ", Data_Fim_Etapa = " + str(DATA_FIM_ETAPA) + ", UF_Etapa = '" + str(UF_ETAPA) + "', Municipio_Etapa = '" +  str(MUNICIPIO_ETAPA) + \
                      "', Endereco_Etapa = '" + str(ENDERECO_ETAPA) + "', CEP_Etapa = '" + str(CEP_ETAPA) + "', Qtde_Etapa = '" + str(QTD_ETAPA) + \
                      "', Unidade_Fornecimento_Etapa = '" + str(UND_FORNECIMENTO_ETAPA) + "', Valor_Etapa = " + str(VL_ETAPA) + \
                      " WHERE Codigo_Etapa = " + str(ID_ETAPA)
            else:               
                sql = "INSERT INTO Etapa_Crono_Fisico(ID_Meta_Crono_Fisico, Codigo_Etapa, Nr_Etapa, Descricao_Etapa, Data_Inicio_Etapa, \
                        Data_Fim_Etapa, UF_Etapa, Municipio_Etapa, Endereco_Etapa, CEP_Etapa, Qtde_Etapa, Unidade_Fornecimento_Etapa, Valor_Etapa) \
                        VALUES(" + str(ID_META) +  ", " + str(ID_ETAPA) + ", " + str(NR_ETAPA) + ",'" + str(DESC_ETAPA) + "', " + \
                        str(DATA_INICIO_ETAPA) + ", " + str(DATA_FIM_ETAPA) + ", '" + str(UF_ETAPA) + "', '" +  str(MUNICIPIO_ETAPA) + "', '" + \
                        str(ENDERECO_ETAPA) + "', '" + str(CEP_ETAPA) + "', '" + str(QTD_ETAPA) + "', '" + str(UND_FORNECIMENTO_ETAPA) + "', " + \
                        str(VL_ETAPA) + ")"
            
            try:
                #cursor = db_connection.cursor()
                db_connection = Connection.connect()
                cursor = Connection.getCursor()
                cursor.execute(sql)
                #cursor.close()
                db_connection.commit()
                numero_etapas_crono = numero_etapas_crono + 1
            except Exception as e:
                print("Erro ao gravar Etapa Crono Física %s" % (ID_ETAPA))
                print(str(e))
                print(sql)
                break
        
    print("Gravadas %d Etapas Crono Físicas" % (numero_etapas_crono))  