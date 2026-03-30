import pandas as pd

df = pd.read_excel(r"C:\Users\henrique\Desktop\Projetos-ITVALLEY\BI\BI\analise_mastertech.xlsx")

df_master = df['mastertech'].dropna().drop_duplicates().astype(str).str.strip().str.lower()
df_pos = df['pos'].dropna().drop_duplicates().astype(str).str.strip().str.lower()

iguais = set(df_master) & set(df_pos)

total_master = len(df_master)
total_pos = len(df_pos)
total_master_pos = len(iguais)

porcentagem_master_pos = round((total_master_pos/total_master)*100,2)


print(f'Compradores Mastertech: {len(df_master)}')
print(f'Compradores Pos: {len(df_pos)}')
print(f'Compradores Master e Pos: {len(iguais)}')
print(f'% de conversão: {porcentagem_master_pos}%')
print(f'Lista de Emails Iguais:{iguais}')
