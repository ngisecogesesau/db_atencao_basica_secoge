import duckdb
import pandas as pd
from datetime import datetime, timedelta

# Funções auxiliares para traduzir dia da semana e mês
def traduzir_dia_semana(dia):
    dias = ['Domingo', 'Segunda-feira', 'Terça-feira', 'Quarta-feira', 'Quinta-feira', 'Sexta-feira', 'Sábado']
    return dias[dia.weekday()]

def traduzir_mes_abreviado(mes):
    meses_abreviados = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set', 'Out', 'Nov', 'Dez']
    return meses_abreviados[mes-1]

def traduzir_mes_completo(mes):
    meses_completos = ['Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho', 'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro']
    return meses_completos[mes-1]

# Função para criar calendário
def create_calendario_table(start_date, end_date):
    delta = end_date - start_date
    data = []

    for i in range(delta.days + 1):
        dia = start_date + timedelta(days=i)
        ano = dia.year
        mes = dia.month
        dia_mes = dia.day
        nome_dia = traduzir_dia_semana(dia)
        dia_semana = dia.weekday()
        mes_abreviado = traduzir_mes_abreviado(mes)
        quadrimestre = (mes - 1) // 4 + 1
        ano_quadrimestre = f"{ano}.{quadrimestre}"
        mes_completo = traduzir_mes_completo(mes)
        mvm = int(dia.strftime('%Y%m'))

        data.append((dia, ano, mes, dia_mes, nome_dia, dia_semana, mes_abreviado, quadrimestre, ano_quadrimestre, mes_completo, mvm))

    return pd.DataFrame(data, columns=[
        'data_dma', 'ano', 'mes', 'dia', 'nome_dia', 'dia_semana', 'mes_abreviado', 'quadrimestre', 
        'ano_quadrimestre', 'mes_completo', 'mvm'
    ])


def insert_data_calendario_table(con):

    con.execute("""
    CREATE TABLE calendario (
        data_dma DATE,
        ano INTEGER,
        mes INTEGER,
        dia INTEGER,
        nome_dia VARCHAR,
        dia_semana INTEGER,
        mes_abreviado VARCHAR,
        quadrimestre INTEGER,
        ano_quadrimestre VARCHAR,
        mes_completo VARCHAR,
        mvm INTEGER
    )
    """)

    start_date = datetime(2020, 1, 1)
    end_date = datetime(2025, 12, 31)
    df_calendario = create_calendario_table(start_date, end_date)
    # Inserindo dados no DuckDB
    con.append('calendario', df_calendario)
    df_calendario_duckdb = con.execute("SELECT * FROM calendario").fetchdf()
    return df_calendario_duckdb


if __name__ == '__main__':
    con = duckdb.connect(database=':memory:')
    df_calendario_duckdb = insert_data_calendario_table(con)
    print(df_calendario_duckdb)
