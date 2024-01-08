import psycopg2

#query mocada, precisa incluir a query original e tambem a taxa 

def query_data(processos):
    
    
    with psycopg2.connect(
        host='localhost',
        port=5432,
        user='postgres',
        password='4321',
        database='cisco'
    ) as conexao:
        with conexao.cursor() as cursor:
            
            query_sql = 'SELECT data, numero_processo FROM processos_datas WHERE numero_processo IN %s'
            try:
                cursor.execute(query_sql, (tuple(processos),))
                print("Consulta data executada com sucesso.")
                resultados = [row for row in cursor.fetchall()] 
              
            except Exception as e:
                print(f"Erro durante a execução da consulta: {e}")
                return None
    
    return resultados