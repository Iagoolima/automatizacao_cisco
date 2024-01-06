from tkinter import *
from tkcalendar import DateEntry

from consulta_gerais import consulta_geral

def imprimir():
    valor_data_inicio = data_inicio.get()
    valor_data_final = data_final.get()
    valor_taxa = taxa.get()
    
    if (valor_data_inicio < valor_data_final) & (valor_taxa != ""):
        consulta_geral(valor_data_inicio, valor_data_final, valor_taxa)
    
    
janela = Tk()
janela.title("Automatização CISCO")

label_data_inicio = Label(janela, text="Data de inicio:")
label_data_inicio.grid(column=0, row=0, pady=13, padx=2)

data_inicio = DateEntry(janela, width=12, background='silver', foreground='white', borderwidth=2, date_pattern='yyyy-mm-dd')
data_inicio.grid(column=1, row=0,pady=13, padx=10)


label_data_final = Label(janela, text="Data de termino:")
label_data_final.grid(column=3, row=0,pady=13, padx=2)


data_final = DateEntry(janela, width=12, background='silver', foreground='white', borderwidth=2, date_pattern='yyyy-mm-dd')
data_final.grid(column=4, row=0,pady=13, padx=10)

label_taxa = Label(janela, text="Valor da Taxa:")
label_taxa.grid(column=0, row=1,pady=13, padx=2)


taxa = Entry(janela)
taxa.grid(column=1, row=1,pady=13, padx=10)


botao_imprimir = Button(janela, text="clique", command=imprimir)
botao_imprimir.grid(column=1, row=2)

janela.mainloop()
