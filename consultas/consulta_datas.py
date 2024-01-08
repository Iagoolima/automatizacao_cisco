from querys.query_data import query_data

def construcao_query_data(processos):
    try:
        resultados = query_data(processos)

        if resultados is not None:
            
            processos_com_data = [elemento for elemento in resultados if elemento[0] is not None]
            processos_sem_data = [elemento[1] for elemento in resultados if elemento[0] is None]
             
            return(processos_com_data, processos_sem_data)

        else:
            print("Erro: A consulta retornou None. Verifique a conexão ou execução da consulta.")
    except Exception as e:
        print(f"Erro durante a execução da consulta: {e}")
    finally:
        if 'cursor' in locals() and resultados is not None:
            resultados.close()
            
    
   
    

    
