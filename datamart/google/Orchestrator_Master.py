import subprocess
import sys
import os

# Lista de orquestradores (em ordem)
orchestrators = [
    os.path.abspath(os.path.join("datamart","google", "ADS","1_bronze","1_Orchestrador", "1_orchestrator_master_TObronze.py")),
    os.path.abspath(os.path.join("datamart","google", "ADS","2_silver","2_Orchestrador", "2_orchestrator_master_bronzeTOsilver.py")),
    os.path.abspath(os.path.join("datamart","google", "ADS","3_gold","3_Orchestrador", "3_orchestrator_master_silverTOgold.py"))
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
