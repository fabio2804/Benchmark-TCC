#!/usr/bin/env python3
"""
Script para executar benchmark em todos os cenários e gerar consolidação
"""

import subprocess
import sys
import os

def executar_benchmark_completo():
    """Executa benchmark para todos os cenários e gera consolidação"""
    
    cenarios = ["pequeno", "medio", "grande"]
    
    print("🚀 Iniciando benchmark completo para todos os cenários...")
    print("="*60)
    
    for cenario in cenarios:
        print(f"\n📊 Executando benchmark para cenário: {cenario}")
        print("-" * 40)
        
        try:
            # Executar o benchmark para cada cenário
            resultado = subprocess.run([
                sys.executable, "main.py", cenario
            ], capture_output=True, text=True, cwd=os.getcwd())
            
            if resultado.returncode == 0:
                print(f"✅ Cenário {cenario} concluído com sucesso")
                # Mostrar apenas as últimas linhas da saída
                linhas = resultado.stdout.strip().split('\n')
                if len(linhas) > 10:
                    print("...")
                    for linha in linhas[-10:]:
                        print(linha)
                else:
                    print(resultado.stdout)
            else:
                print(f"❌ Erro no cenário {cenario}:")
                print(resultado.stderr)
                
        except Exception as e:
            print(f"❌ Erro ao executar cenário {cenario}: {e}")
    
    print("\n" + "="*60)
    print("🎉 Benchmark completo finalizado!")
    print("📄 Verifique o arquivo 'resultados_geral.csv' para os resultados consolidados")
    
    # Verificar se o arquivo foi criado
    if os.path.exists("resultados_geral.csv"):
        print("✅ Arquivo consolidado criado com sucesso!")
    else:
        print("⚠️  Arquivo consolidado não encontrado")

if __name__ == "__main__":
    executar_benchmark_completo()