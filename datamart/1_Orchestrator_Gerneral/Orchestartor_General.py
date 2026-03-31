import subprocess
import sys
import os

# Lista de orquestradores (em ordem)
orchestrators = [
    os.path.abspath(os.path.join("datamart","facebook","Orchestrator_Master.py" )),
    os.path.abspath(os.path.join("datamart","google","Orchestrator_Master.py" )),
    os.path.abspath(os.path.join("datamart","redes_sociais","01_Orchestrator","00_Orchestrator_Master.py" )),
    os.path.abspath(os.path.join("datamart","calls_Gabriel","Orschestrator","01_Orchestrator.py" )),
    os.path.abspath(os.path.join("datamart","hotmart_mastertech","Orchestrator_Master.py" )),
    os.path.abspath(os.path.join("datamart","voomp","Orchestrator_Master.py" )),
    os.path.abspath(os.path.join("datamart","pipedrive","Orchestrator_master.py" )),
    os.path.abspath(os.path.join("datamart","moedas","moedas.py" ))
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
