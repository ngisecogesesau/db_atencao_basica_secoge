import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score

# Função para carregar dados
def load_data():
    # Ler dados de treinamento
    df_train = pd.read_excel('treino_tipo_unidade.xlsx')  # Substitua pelo caminho real do seu arquivo de treinamento
    # Ler dados a serem preenchidos
    df_to_predict = pd.read_excel('teste.xlsx')  # Substitua pelo caminho real do seu arquivo a ser preenchido
    return df_train, df_to_predict

# Função para preparar os dados
def prepare_data(df, target_column=None):
    # Se target_column for fornecida, remova-a temporariamente do DataFrame para evitar codificação
    if target_column and target_column in df.columns:
        target = df[target_column]
        df = df.drop(columns=[target_column])
    else:
        target = None

    # Tratar todas as colunas categóricas como texto antes de codificar
    for col in df.columns:
        df[col] = df[col].astype(str)
    
    # Codificar todas as colunas categóricas, exceto target_column
    df = pd.get_dummies(df, drop_first=True)

    # Recolocar a coluna de alvo no DataFrame se ela foi removida
    if target_column and target is not None:
        df[target_column] = target
    
    return df

# Carregar os dados
df_train, df_to_predict = load_data()

# Função para preencher valores ausentes de uma coluna alvo
def fill_missing_values(df_train, df_to_predict, target_column):
    df_train_complete = df_train.dropna(subset=[target_column])
    df_train_missing = df_train[df_train[target_column].isna()]

    # Preparar dados completos para treinamento
    df_train_complete_prepared = prepare_data(df_train_complete, target_column=target_column)

    # Separar características (X) e alvo (y)
    X = df_train_complete_prepared.drop(columns=[target_column])
    y = df_train_complete_prepared[target_column]

    # Dividir dados de treinamento e teste
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Treinamento do modelo
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Avaliação do modelo
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f'Accuracy for {target_column}: {accuracy:.2f}')

    # Preparar dados a serem preenchidos
    df_to_predict_prepared = prepare_data(df_to_predict)

    # Garantir que os dados de previsão tenham as mesmas colunas que os dados de treinamento
    missing_cols = set(X.columns) - set(df_to_predict_prepared.columns)
    for col in missing_cols:
        df_to_predict_prepared[col] = 0
    df_to_predict_prepared = df_to_predict_prepared[X.columns]

    # Fazer previsões
    predictions = model.predict(df_to_predict_prepared)
    
    # Adicionar previsões ao DataFrame original
    df_to_predict[target_column] = predictions
    
    return df_to_predict

# Preencher valores ausentes de 'tipo_unidade'
df_to_predict = fill_missing_values(df_train, df_to_predict, 'tipo_unidade')

# Salvar o DataFrame preenchido em um novo arquivo Excel
df_to_predict.to_excel('dados_preenchidos_tipo_unidade.xlsx', index=False)
print("Dados preenchidos salvos em 'dados_preenchidos.xlsx'")
