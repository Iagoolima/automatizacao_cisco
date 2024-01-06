from consulta_datas import construcao_query_data
from consulta_principal import construcao_query_principal
from atualizacao_lista import substituir_datas_na_lista


def consulta_geral(data_inicio, data_final, taxa):
    
    try:
        resultado_principal = construcao_query_principal(data_inicio, data_final, taxa)
        resultado_lista_geral = resultado_principal[0]
        resultado_processo_sem_data_consultar = resultado_principal[1]
        
        
       
        
        resultado_data = construcao_query_data(resultado_processo_sem_data_consultar)
        processo_com_data = resultado_data[0]
        
        # print('lista completa que precisa completar a data do processo', resultado_lista_geral)
        # print('processos com data que deve susbtituir na lista a cima', processo_com_data)
        # print(resultado_lista_geral)
        
        lista_finalizada = substituir_datas_na_lista(resultado_lista_geral, processo_com_data)
        
        resultado_ge_sem_data = [elemento[2] for elemento in lista_finalizada if isinstance(elemento[2], str) and elemento[2].startswith("GE") and elemento[1] is None]
        resultado_gi_sem_data = [elemento[2] for elemento in lista_finalizada if isinstance(elemento[2], str) and elemento[2].startswith("GI") and elemento[1] is None]
       
        print('////////////////////////////////')
        print(lista_finalizada)
        print('////////////////////////////////')
        print('ge sem data', resultado_ge_sem_data)
        print('////////////////////////////////')
        print('gi sem data', resultado_gi_sem_data )
        
        
    except Exception as e:
        print(f"Erro durante a execução da consulta: {e}")


    
 
    
    