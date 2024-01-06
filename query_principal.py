import psycopg2

#query mocada, precisa incluir a query original e tambem a taxa 

def query_principal(data_inicio, data_final):
    
    
    with psycopg2.connect(
        host='localhost',
        port=5432,
        user='postgres',
        password='4321',
        database='cisco'
    ) as conexao:
        with conexao.cursor() as cursor:
            #query_sql = '''SELECT * FROM processos WHERE data BETWEEN %s and %s'''
            query_sql = 'SELECT * FROM processos'
            try:
                #cursor.execute(query_sql, (data_inicio, data_final))
                cursor.execute(query_sql)
                print("Consulta executada com sucesso.")
                resultados = [row for row in cursor.fetchall()]  # Crie uma lista a partir dos resultados
            except Exception as e:
                print(f"Erro durante a execução da consulta: {e}")
                return None
    
    return resultados