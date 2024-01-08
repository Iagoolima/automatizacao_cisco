from querys.query_principal import query_principal

def construcao_query_principal(data_inicio, data_final, taxa):
    try:
        resultados = query_principal(data_inicio, data_final)

        if resultados is not None:
            
           elementos_ge_data_vazia = [elemento[3] for elemento in resultados if isinstance(elemento[2], str) and elemento[2].startswith("GE") and elemento[1] is None]

           return(resultados, elementos_ge_data_vazia)
       
       
        else:
            print("Erro: A consulta retornou None. Verifique a conexão ou execução da consulta.")
    except Exception as e:
        print(f"Erro durante a execução da consulta: {e}")
    finally:
        if 'cursor' in locals() and resultados is not None:
            resultados.close()
            
    
   
    

    
