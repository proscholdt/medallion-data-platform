import subprocess
import sys
import os

orchestrators = [
    # os.path.abspath(os.path.join("datamart", "voomp", "2_silver", "2_TO_carregados", "7_f_projecao_TO_carregados.py")),
    os.path.abspath(os.path.join("datamart", "voomp", "2_silver", "2_TO_carregados", "8_f_vendas_TO_carregados.py"))
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
