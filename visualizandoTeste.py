import polars as pl

# Lê o arquivo Parquet
df = pl.read_parquet(r"C:\Users\henrique\Downloads\leadsActive (1).parquet"
                      )

# Mostrar colunas, tipos e contagem de nulos/em branco/vazios e NÃO nulos
print("📋 Análise de colunas:\n")

total_rows = df.height

for col_name, dtype in df.schema.items():
    # Conta nulos (None ou NaN)
    n_nulls = df[col_name].null_count()
    
    # Conta não nulos
    n_not_nulls = total_rows - n_nulls

    # Conta strings em branco ou só espaços (para colunas de texto)
    if dtype == pl.Utf8:
        n_blank = df.filter(
            (pl.col(col_name).str.strip_chars().is_in(["", None]))
        ).shape[0]
    else:
        n_blank = 0  # não aplica para colunas não-texto

    print(
        f"{col_name}: {dtype} | "
        f"Nulos: {n_nulls} | Não nulos: {n_not_nulls} | Em branco/vazio: {n_blank}"
    )

print("\n🔢 Shape do DataFrame:", df.shape)

# Mostrar os primeiros registros para visualização
print("\n📄registros do DataFrame:\n")
print(df)



# import polars as pl
# import pandas as pd

# # Lê o arquivo Parquet com Polars
# df = pl.read_parquet(r"C:\Users\henrique\Downloads\2025-07-08_dados_ads.parquet")

# # Converte para pandas para exibição completa
# df_pd = df.to_pandas()

# # Exibe todos os dados em formato tabular
# pd.set_option("display.max_rows", None)
# pd.set_option("display.max_columns", None)
# pd.set_option("display.width", None)

# print(df_pd.to_string(index=False))





