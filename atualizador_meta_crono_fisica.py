from csv import reader
import mysql.connector
#from database.gerenciador_conexao_bd import connect
from database.gerenciador_conexao_bd import Connection
from util.dateUtil import converteData
from util.stringUtil import checarCampoVazio, removeNonASCIICharacters
from importersiconv.gerenciador_consultas import getIDConvenio, getIDProposta, getIDMetaCronoFisica, obtemIDPropostaTbTemp

def atualizarMetasCronoFisicas(arquivo_csv_meta_crono_fisica):
    db_connection = Connection.connect()

    numero_linhas_csv = 0
    numero_metas_crono = 0
    with open(arquivo_csv_meta_crono_fisica, 'r', encoding="utf8") as arquivo_csv:
        csv_reader = reader(arquivo_csv, delimiter=';')
        for linha in csv_reader:
            ##Leitura dos dados da planilha
            if numero_linhas_csv == 0:
                numero_linhas_csv = numero_linhas_csv + 1
                continue
            numero_linhas_csv = numero_linhas_csv + 1
            #Campos do CSV
            ID_META = linha[0].strip()
            ID_PROPOSTA = linha[1].strip() 
            ID_PROPOSTA = getIDProposta(linha[1].strip())
            #Proposta não encontrada
            if ID_PROPOSTA == 0:
                continue;
            NR_CONVENIO = linha[2].strip()
            ID_CONVENIO = "NULL";
            if NR_CONVENIO != '':
                ID_CONVENIO = getIDConvenio(NR_CONVENIO)
            COD_PROGRAMA = linha[3].strip()
            NOME_PROGRAMA = ((removeNonASCIICharacters(linha[4].strip().replace("'", "\\'"))).replace('\\','')).replace("'",'')
            NR_META = linha[5].strip()
            TIPO_META = linha[6].strip()
            DESC_META = ((removeNonASCIICharacters(linha[7].strip().replace("'", "\\'"))).replace('\\','')).replace("'",'')
            DATA_INICIO_META = converteData(linha[8].strip())
            DATA_FIM_META = converteData(linha[9].strip())
            UF_META = linha[10].strip()
            MUNICIPIO_META = ((removeNonASCIICharacters(linha[11].strip().replace("'", "\\'"))).replace('\\','')).replace("'",'')
            ENDERECO_META = ((removeNonASCIICharacters(linha[12].strip().replace("'", "\\'"))).replace('\\','')).replace("'",'')
            CEP_META = linha[13].strip()
            QTD_META = linha[14].strip()
            UND_FORNECIMENTO_META = linha[15].strip()
            VL_META = checarCampoVazio(linha[16].strip().replace(",", "."))
            
            if obtemIDPropostaTbTemp(ID_PROPOSTA) and getIDMetaCronoFisica(ID_META):
                
                sql = "UPDATE Meta_Crono_Fisico SET ID_Proposta = " + str(ID_PROPOSTA) + ", ID_Convenio = "  + str(ID_CONVENIO) + \
                        ", Tipo_Meta = '" + str(TIPO_META) + "', Codigo_Meta_Crono_Fisico = " + str(ID_META) + \
                        ", Cod_Programa = '" + str(COD_PROGRAMA) + "', Nome_Programa = '" + str(NOME_PROGRAMA) + \
                        "', Nr_Meta = " + str(NR_META) + ", Descricao_Meta = '" + str(DESC_META) + "', Data_Inicio_Meta = " + str(DATA_INICIO_META) + \
                        ", Data_Fim_Meta = " + str(DATA_FIM_META) + ", UF_Meta = '" + str(UF_META) + "', Municipio_Meta = '" + str(MUNICIPIO_META) + \
                        "', Endereco_Meta = '" + str(ENDERECO_META) + "', CEP_Meta = '" + str(CEP_META) + "', Qtde_Meta = '" + str(QTD_META) + \
                        "', Unidade_Fornecimento_Meta = '" + str(UND_FORNECIMENTO_META) + "', Valor_Meta =" + str(VL_META) + \
                        " WHERE ID_Proposta = " + str(ID_PROPOSTA) + " AND Codigo_Meta_Crono_Fisico = " + str(ID_META)
            
            elif obtemIDPropostaTbTemp(ID_PROPOSTA):
            
                sql = "INSERT INTO Meta_Crono_Fisico(ID_Proposta, ID_Convenio, Tipo_Meta, Codigo_Meta_Crono_Fisico, \
                        Cod_Programa, Nome_Programa, Nr_Meta, Descricao_Meta, Data_Inicio_Meta, Data_Fim_Meta, \
                        UF_Meta, Municipio_Meta, Endereco_Meta, CEP_Meta, Qtde_Meta, Unidade_Fornecimento_Meta, Valor_Meta) VALUES(" + \
                        str(ID_PROPOSTA) + ", " + str(ID_CONVENIO) + ", '" + str(TIPO_META) + "', " + str(ID_META) + ", '" + \
                        str(COD_PROGRAMA) + "', '" + str(NOME_PROGRAMA) + "', " + str(NR_META) + ", '" + str(DESC_META) + "', " + \
                        str(DATA_INICIO_META) + ", " + str(DATA_FIM_META) + ", '" + str(UF_META) + "', '" + str(MUNICIPIO_META) + "', '" + \
                        str(ENDERECO_META) + "', '" + str(CEP_META) + "', '" + str(QTD_META) + "', '" + str(UND_FORNECIMENTO_META) + "', " + \
                        str(VL_META) + ")"
            else:
                continue
            
            try:
                  #cursor = db_connection.cursor()
                db_connection = Connection.connect()
                cursor = Connection.getCursor()
                cursor.execute(sql)
                #cursor.close()
                db_connection.commit()
                numero_metas_crono = numero_metas_crono + 1
            except Exception as e:
                print("Erro ao gravar Meta Crono Física %s" % (ID_META))
                print(str(e))
                print(sql)
                continue
        
    
    print("Gravadas %d Metas Crono Físicas" % (numero_metas_crono)) 