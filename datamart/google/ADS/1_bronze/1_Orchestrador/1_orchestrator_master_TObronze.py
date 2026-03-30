import subprocess
import sys
import os

# Lista de scripts (em ordem) na pasta Transformation
scripts = [
    "0_campanhas_to_bronze.py",
    "1_grupoAnuncio_to_bronze.py",
    "2_anuncios_to_bronze.py",
    "03_keywords_to_bronze.py",
    "4_campanhas_bronze_to_silver.py",
    "5_keywords_bronze_to_silver.py",
    "6_grupos_bronze_to_silver.py",
    "7_anuncios_bronze_to_silver.py",
    "8_campanhas_bronze_to_carregados.py",
    "9_anuncios_bronze_to_carregados.py",
    "10_grupos_bronze_to_carregados.py",
    "11_keywords_bronze_to_carregados.py",
    "12_publicoAlvo_to_bronze.py",
    "13_publicoAlvo_bronze_to_silver.py",
    "14_publicoAlvo_bronze_to_carregados.py"
]

# Caminho base: pasta Transformation (relativo a este arquivo)
base_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "Transformation")
)

print(f"🚀 Iniciando execução sequencial dos scripts na pasta: {base_path}\n")

for idx, script in enumerate(scripts, 1):
    script_path = os.path.join(base_path, script)
    print(f"▶️ [{idx}/{len(scripts)}] Executando: {script_path}")

    try:
        subprocess.run(
            [sys.executable, script_path],
            check=True
        )
        print(f"✅ [{idx}] Concluído com sucesso.\n")

    except subprocess.CalledProcessError as e:
        print(f"❌ Erro ao executar {script_path}")
        print("\n🛑 Execução interrompida devido a erro.")
        sys.exit(1)

print("🎉 Todos os scripts foram executados com sucesso!")
