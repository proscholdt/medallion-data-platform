import subprocess
import sys
import os

# Lista de orquestradores (em ordem)
orchestrators = [
    os.path.abspath(os.path.join("datamart","hotmart_mastertech", "1_bronze","1_Orchestrator", "01_or.py")),
    os.path.abspath(os.path.join("datamart","hotmart_mastertech", "2_silver","2_Orchestrator", "02_or.py")),
    os.path.abspath(os.path.join("datamart","hotmart_mastertech", "3_gold","3_Orchestrator", "03_or.py")),

]

print("🚀 Iniciando execução sequencial dos orquestradores...\n")

for idx, orchestrator in enumerate(orchestrators, 1):
    print(f"▶️ [{idx}/{len(orchestrators)}] Executando: {orchestrator}")

    try:
        subprocess.run(
            [sys.executable, orchestrator],
            check=True
        )
        print(f"✅ [{idx}] Concluído com sucesso.\n")

    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar {orchestrator}")
        print("\nExecução interrompida devido a erro.")
        sys.exit(1)

print("🎉 Todos os orquestradores foram executados com sucesso!")
