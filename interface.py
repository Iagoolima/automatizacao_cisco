from tkinter import *
from tkcalendar import DateEntry
from tkinter import ttk, messagebox
from consulta_gerais import consulta_geral
from datetime import datetime, timedelta
from calendar import monthrange

janela_confirmacao = None  # Declaração global
botao_imprimir = None  # Declaração global
nova_data_final_label = None  # Declaração global

def centralizar_janela(janela, largura, altura):
    largura_tela = janela.winfo_screenwidth()
    altura_tela = janela.winfo_screenheight()

    x = (largura_tela - largura) // 2
    y = (altura_tela - altura) // 2

    janela.geometry(f'{largura}x{altura}+{x}+{y}')

def mostrar_janela_carregamento():
    janela_carregamento = Toplevel(janela)
    janela_carregamento.title("Aguarde")

    largura = 300
    altura = 100

    label_aguarde = Label(janela_carregamento, text="Aguarde, consultando dados...")
    label_aguarde.pack(padx=20, pady=20)

    centralizar_janela(janela_carregamento, largura, altura)

    # Adiciona um pequeno atraso antes de mostrar a janela
    janela_carregamento.update()

    return janela_carregamento

def fechar_janela_carregamento(janela_carregamento):
    janela_carregamento.destroy()

def copiar_lista(listbox):
    items = listbox.get(0, END)
    unique_items = list(set(items))  # Remover duplicatas
    clipboard_text = "\n".join(unique_items)
    janela.clipboard_clear()
    janela.clipboard_append(clipboard_text)
    janela.update()

def definir_data_final(event):
    # Evento chamado ao selecionar a data de início
    valor_data_inicio = data_inicio.get()
    
    # Verifica se a data de início está vazia
    if valor_data_inicio:
        data_inicio_dt = datetime.strptime(valor_data_inicio, '%d-%m-%Y')
        
        # Obtém o último dia do mês da data de início
        ultimo_dia_mes = monthrange(data_inicio_dt.year, data_inicio_dt.month)[1]
        
        # Atualiza o rótulo com a nova data final
        nova_data_final_label.config(text=f'{ultimo_dia_mes:02d}-{data_inicio_dt.month:02d}-{data_inicio_dt.year}')
    else:
        nova_data_final_label.config(text="Escolha a data de início")  # Se a data de início estiver vazia, limpa a data final

def validar_data():
    valor_data = data_inicio.get()
    try:
        # Tenta converter a string para um objeto datetime
        datetime.strptime(valor_data, '%d-%m-%Y')
        return True
    except ValueError:
        messagebox.showerror("Erro", "Data inválida. Por favor, insira uma data no formato dd-mm-yyyy.")
        return False

def validar_taxa():
    valor_taxa = taxa.get()

    if not valor_taxa.replace('.', '').isdigit():
        messagebox.showerror("Erro", "Por favor, insira um valor numérico válido para a taxa.")
        return False
    
    if len(valor_taxa) > 10:
        messagebox.showerror("Erro", "A taxa deve ter no máximo 10 caracteres.")
        return False

    return True

def confirmar_acao():
    global janela_confirmacao  # Tornar a variável global
    global botao_imprimir
    
    if not validar_data():
        return

    if not validar_taxa():
        return

    valor_data_inicio = data_inicio.get()
    valor_data_final = nova_data_final_label.cget("text")
    valor_taxa = taxa.get()
    
    if valor_data_inicio and valor_taxa and valor_data_final:

        # Desativar o botão antes de iniciar a ação
        botao_imprimir.config(state=DISABLED)

        janela_confirmacao = Toplevel(janela)
        janela_confirmacao.title("Confirmação")

        largura = 400
        altura = 200

        centralizar_janela(janela_confirmacao, largura, altura)

        Label(janela_confirmacao, text="Deseja realmente realizar esta ação?").pack(pady=20)
        Label(janela_confirmacao,
              text=f'Data de inicio: {valor_data_inicio}\nData de termino: {valor_data_final}\nTaxa: {valor_taxa}').pack()
        Label(janela_confirmacao,
              text=f'O processo após inicializado não terá como cancelar, e por isso confirme').pack()

        btn_confirmar = Button(janela_confirmacao, text="Sim", command=imprimir)
        btn_confirmar.pack(side=LEFT, padx=10)

        btn_cancelar = Button(janela_confirmacao, text="Cancelar", command=lambda: cancelar_acao())
        btn_cancelar.pack(side=RIGHT, padx=10)

def imprimir():
    global janela_confirmacao  # Tornar a variável global
    global botao_imprimir

    janela_confirmacao.destroy()

    valor_data_inicio = data_inicio.get()
    valor_data_final = nova_data_final_label.cget("text")
    valor_taxa = taxa.get()

    data_inicio_dt = datetime.strptime(valor_data_inicio, '%d-%m-%Y')
    data_final_dt = datetime.strptime(valor_data_final, '%d-%m-%Y')

    if (data_inicio_dt < data_final_dt) and (valor_taxa != ""):
        janela_carregamento = mostrar_janela_carregamento()
        janela.update_idletasks()

        resultado_ge_sem_data, resultado_gi_sem_data = consulta_geral(valor_data_inicio, valor_data_final, valor_taxa)

        fechar_janela_carregamento(janela_carregamento)

        # Adicione a exibição dos resultados na interface gráfica
        listbox_ge = Listbox(janela, selectmode=SINGLE, width=30, exportselection=0, height=10)
        listbox_ge.grid(column=0, row=6, pady=13, padx=2)

        for item in resultado_ge_sem_data:
            listbox_ge.insert(END, item)

        btn_copiar_ge = Button(janela, text="Copiar GE", command=lambda: copiar_lista(listbox_ge))
        btn_copiar_ge.grid(column=1, row=6, pady=13, padx=2)

        listbox_gi = Listbox(janela, selectmode=SINGLE, width=30, exportselection=0, height=10)
        listbox_gi.grid(column=0, row=8, pady=13, padx=2)

        for item in resultado_gi_sem_data:
            listbox_gi.insert(END, item)

        btn_copiar_gi = Button(janela, text="Copiar GI", command=lambda: copiar_lista(listbox_gi))
        btn_copiar_gi.grid(column=1, row=8, pady=13, padx=2)

    # Reativar o botão após a conclusão da ação
    botao_imprimir.config(state=NORMAL)

def cancelar_acao():
    global janela_confirmacao
    global botao_imprimir

    # Reativar o botão se o usuário cancelar a ação
    botao_imprimir.config(state=NORMAL)

    if janela_confirmacao:
        janela_confirmacao.destroy()

# Configuração da janela principal
janela = Tk()
janela.title("Automatização CISCO")

largura_janela = 800
altura_janela = 500

centralizar_janela(janela, largura_janela, altura_janela)

label_title = Label(janela, text="Relatório CISCO:", font=("Helvetica", 16, "bold"))
label_title.grid(column=0, row=0, columnspan=2, pady=10)

label_instrucoes = Label(janela, text="Sistema para gerar relatório da CISCO mensalmente\nPara utilizar o sistema informe a data e o valor da taxa para inicializar.\nCaso não preenchido, ele não será inicializado.\nO sistema leva um tempo em média de 25 minutos, por isso, revise os valores inseridos.\nApós ser inicializado, ele ficará indisponível para cancelar.",
                         font=("Helvetica", 12), justify=LEFT)
label_instrucoes.grid(column=0, row=1, columnspan=5, pady=10)

label_data_inicio = Label(janela, text="Data de início:")
label_data_inicio.grid(column=0, row=3, pady=13, padx=2)

data_inicio = DateEntry(janela, width=12, background='silver', foreground='white', borderwidth=2,
                        date_pattern='dd-mm-yyyy', state='normal', date=None)
data_inicio.grid(column=1, row=3, pady=13, padx=10)

label_data_final = Label(janela, text="Data de término:")
label_data_final.grid(column=3, row=3, pady=13, padx=2)

# Alterado para Label
nova_data_final_label = Label(janela, text="")
nova_data_final_label.grid(column=4, row=3, pady=13, padx=10)

# Adiciona o evento de mudança à data de início
data_inicio.bind("<<DateEntrySelected>>", definir_data_final)

label_taxa = Label(janela, text="Valor da Taxa:")
label_taxa.grid(column=0, row=5, pady=13, padx=2)

# Alterado para Entry
taxa = Entry(janela)
taxa.grid(column=1, row=5, pady=13, padx=10)

botao_imprimir = Button(janela, text="Confirmar", command=confirmar_acao)
botao_imprimir.grid(column=1, row=6)

janela.mainloop()
