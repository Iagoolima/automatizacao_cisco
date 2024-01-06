from tkinter import *
from tkcalendar import DateEntry

def exibir_data_inicio(event):
    selected_date = data_inicio.get_date()
    label_data_inicio.config(text=f"data selecionada: {selected_date}")

def exibir_data_final(event):
    selected_date = data_final.get_date()
    label_data_termino.config(text=f"Data de término: {selected_date}")

janela = Tk()
janela.title("Automatização CISCO")

label_data_inicio = Label(janela, text="Digite a data de inicio:")
label_data_inicio.grid(column=0, row=0, pady=10, padx=10)

data_inicio = DateEntry(janela, width=12, background='darkblue', foreground='white', borderwidth=2, year=4)
data_inicio.grid(column=1, row=0,pady=10, padx=10)
data_inicio.bind("<<DateEntrySelected>>", exibir_data_inicio)



label_data_termino = Label(janela, text="Digite a data de termino:")
label_data_termino.grid(column=3, row=0,pady=10, padx=10)


data_final = DateEntry(janela, width=12, background='darkblue', foreground='white', borderwidth=2, year=4)
data_final.grid(column=4, row=0,pady=10, padx=10)
data_final.bind("<<DateEntrySelected>>", exibir_data_final)


janela.mainloop()
