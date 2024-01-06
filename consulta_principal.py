import psycopg2

from consulta_datas import construcao_query_data

def construcao_query(data_inicio, data_final, taxa):

    conexao = psycopg2.connect(
        host='localhost',
        port=5432,  # Substitua pela porta do seu PostgreSQL
        user='postgres',
        password='4321',
        database='cisco'
    )
    cursor = conexao.cursor()
    
    # comentario pra usar de filtro
    # query_sql = '''SELECT * FROM processos WHERE data BETWEEN %s and %s'''
    #cursor.execute(query_sql, (data_inicio, data_final))
    
    query_sql = 'SELECT * FROM processos'
    cursor.execute(query_sql)
    
    resultados = cursor.fetchall()
    
    elementos_nulos = [elemento for elemento in resultados if not elemento[1]]
    
    ultimos_elementos = [elemento[-1] for elemento in elementos_nulos]
    
    

    construcao_query_data(ultimos_elementos)
    
    INSERT INTO processos (data, tipo, numero_processo)
VALUES
  ('2023-12-01', 'GE1234-23', 987654),
  ('2023-11-15', 'GI5678-22', 123456),
  ('2023-10-20', 'GE9101-23', 789012),
  (NULL, 'GI1121-22', 345678),
  (NULL, 'GE3141-23', 901234),
  (NULL, 'GI5161-22', 234567),
  ('2024-01-10', 'GI7181-22', 876543),
  ('2024-02-05', 'GE9202-23', 567890),
  ('2024-03-15', 'GI1234-22', 123789);
  

    
