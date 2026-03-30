import subprocess
import sys
import os

orchestrators = [
    os.path.abspath(os.path.join("datamart", "voomp", "2_silver", "1_TO_gold", "00_projetadas_silverTOgold.py")),
    os.path.abspath(os.path.join("datamart", "voomp", "2_silver", "1_TO_gold", "1_dim_afiliado_silverTOgold.py")),
    os.path.abspath(os.path.join("datamart", "voomp", "2_silver", "1_TO_gold", "2_dim_cliente_silverTOgold.py")),
    os.path.abspath(os.path.join("datamart", "voomp", "2_silver", "1_TO_gold", "3_dim_oferta_silverTOgold.py")),
    os.path.abspath(os.path.join("datamart", "voomp", "2_silver", "1_TO_gold", "4_dim_produto_silverTOgold.py")),
    # os.path.abspath(os.path.join("datamart", "voomp", "2_silver", "1_TO_gold", "5_f_projecao_silverTOgold.py")),
    os.path.abspath(os.path.join("datamart", "voomp", "2_silver", "1_TO_gold", "6_f_vendas_silverTOgold.py")),
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
