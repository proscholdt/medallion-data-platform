import subprocess
import sys
import os

# Lista de scripts (em ordem) na pasta transformador
scripts = [
    "1_move_silverTOgold.py",
    "2_move_TO_carregados.py",
    "3_all_person_silver_TOgold.py",
    "4_all_person_TO_carregados.py",
    "05_all_pipelines_stages_dimPipeline_TO_gold.py",
    "06_all_pipelines_stages_dimStage_TO_gold.py",
    "07_all_pipelines_stages_TO_carregados.py",
    "08_scd2_stages.py"
]

# Caminho base: pasta transformador (relativo a este arquivo)
base_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "transformador")
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
