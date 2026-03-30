import subprocess
import sys
import os

# Lista de scripts (em ordem) na pasta Transformation
scripts = [
    "01_dim_anuncio.py",
    "02_dim_campanha.py",
    "03_dim_dispositivo.py",
    "04_dim_grupoAnuncio.py",
    "05_dim_rede.py",
    "06_fato_campanha.py",
    "07_fato_grupoAnuncio.py",
    "08_fato_keyords.py",
    "09_fato_anuncio.py",
    "10_fato_publicoAlvo.py",
    "11_dim_publicoAlvo.py"
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
