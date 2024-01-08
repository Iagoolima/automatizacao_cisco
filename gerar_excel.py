import pandas as pd


def gerar_arquivo_excel(lista, data_inicio, data_final):
    # Converte a lista para um DataFrame do Pandas
    df = pd.DataFrame(lista, columns=['ID', 'Data', 'Tipo', 'Processo'])

    # Salva o DataFrame como um arquivo Excel
    df.to_excel(f'relatorios/{data_inicio} até {data_final} - Relatorio Cisco.xlsx', index=False)


