def substituir_datas_na_lista(lista_geral, processos_com_data):
    for processo_com_data_item in processos_com_data:
        for i, resultado_lista_item in enumerate(lista_geral):
            if resultado_lista_item[3] == processo_com_data_item[1]:  # Comparação pelo número do processo
                lista_geral[i] = (
                    resultado_lista_item[0],
                    processo_com_data_item[0],
                    resultado_lista_item[2],
                    resultado_lista_item[3]
                )
    return lista_geral
