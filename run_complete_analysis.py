#!/usr/bin/env python3
"""
Script principal para executar todas as análises e gerar gráficos para TCC
Coordena benchmark + análise + visualizações
"""

import subprocess
import sys
import os
from pathlib import Path

def install_dependencies():
    """Instala dependências necessárias"""
    print("📦 Verificando e instalando dependências...")
    
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Dependências instaladas com sucesso!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Erro ao instalar dependências: {e}")
        return False

def run_full_benchmark():
    """Executa benchmark completo em todos os cenários"""
    print("\n🚀 Executando benchmark completo...")
    
    try:
        result = subprocess.run([sys.executable, "run_all_benchmarks.py"], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Benchmark completo executado com sucesso!")
            return True
        else:
            print(f"❌ Erro no benchmark: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao executar benchmark: {e}")
        return False

def generate_static_plots():
    """Gera gráficos estáticos para TCC"""
    print("\n📊 Gerando gráficos estáticos...")
    
    try:
        result = subprocess.run([sys.executable, "generate_plots.py"], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Gráficos estáticos gerados com sucesso!")
            print(result.stdout)
            return True
        else:
            print(f"❌ Erro na geração de gráficos: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao gerar gráficos: {e}")
        return False

def generate_interactive_plots():
    """Gera gráficos interativos (opcional)"""
    print("\n🌐 Gerando gráficos interativos...")
    
    try:
        result = subprocess.run([sys.executable, "generate_interactive_plots.py"], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            print("✅ Gráficos interativos gerados com sucesso!")
            return True
        else:
            print(f"⚠️  Gráficos interativos falharam (plotly pode não estar instalado): {result.stderr}")
            return False
            
    except Exception as e:
        print(f"⚠️  Erro ao gerar gráficos interativos: {e}")
        return False

def create_final_report():
    """Cria relatório final com todos os resultados"""
    print("\n📋 Criando relatório final...")
    
    report_content = """# Relatório Final - Benchmark ETL Engines

## Resumo Executivo
Este relatório apresenta os resultados da análise comparativa de performance entre as engines de processamento de dados: DuckDB, Pandas e Polars.

## Arquivos Gerados

### Dados Brutos
- `resultados_geral.csv` - Todos os resultados consolidados
- `resultados_geral_agrupado.csv` - Resultados com estatísticas agrupadas
- `benchmark_resultados_pequeno.csv` - Resultados do cenário pequeno
- `benchmark_resultados_medio.csv` - Resultados do cenário médio  
- `benchmark_resultados_grande.csv` - Resultados do cenário grande

### Visualizações Estáticas (Pasta: graficos_benchmark/)
- `tempo_por_engine_cenario.png` - Comparação de tempo por engine e cenário
- `memoria_por_engine_cenario.png` - Comparação de memória por engine e cenário
- `escalabilidade_tempo.png` - Análise de escalabilidade do tempo
- `escalabilidade_memoria.png` - Análise de escalabilidade da memória
- `heatmap_performance.png` - Mapa de calor da performance
- `radar_chart_engines.png` - Comparação multidimensional
- `tempo_vs_memoria.png` - Análise de trade-offs
- `tabela_resumo.csv` - Tabela resumo para inclusão no TCC

### Visualizações Interativas (Pasta: graficos_interativos/)
- `dashboard_interativo.html` - Dashboard completo interativo
- `analise_3d.html` - Análise 3D de performance
- `animacao_performance.html` - Animação da evolução por cenário

## Recomendações para uso no TCC

### Figuras Principais Recomendadas:
1. **tempo_por_engine_cenario.png** - Para mostrar comparação direta de performance
2. **escalabilidade_tempo.png** - Para demonstrar como cada engine escala
3. **memoria_por_engine_cenario.png** - Para análise de eficiência de memória
4. **radar_chart_engines.png** - Para comparação multidimensional
5. **heatmap_performance.png** - Para visão geral da performance

### Tabelas:
- Use `tabela_resumo.csv` para criar tabelas numéricas no TCC

### Análise Interativa:
- Os arquivos HTML podem ser usados em apresentações
- Demonstram interatividade e profundidade da análise

## Metodologia
- Cada operação foi executada 10 vezes para garantir consistência
- Medições incluem tempo de execução e uso de memória
- Três cenários testados: pequeno, médio e grande
- Garbage collection executado entre medições para precisão

## Dados Utilizados
- Microdados do ENEM 2023 (fonte: INEP)
- Datasets em três tamanhos para análise de escalabilidade
- Operações típicas de ETL: leitura, filtros, joins, agregações, escrita

---
Relatório gerado automaticamente pelo sistema de benchmark.
"""
    
    try:
        with open("RELATORIO_FINAL.md", "w", encoding="utf-8") as f:
            f.write(report_content)
        
        print("✅ Relatório final criado: RELATORIO_FINAL.md")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao criar relatório: {e}")
        return False

def main():
    """Função principal que executa todo o pipeline"""
    print("🎓 SISTEMA COMPLETO DE BENCHMARK PARA TCC")
    print("=" * 60)
    print("📊 Este script irá:")
    print("  1. Instalar dependências necessárias")
    print("  2. Executar benchmark completo (pode demorar)")
    print("  3. Gerar gráficos estáticos para TCC")
    print("  4. Gerar gráficos interativos (opcional)")
    print("  5. Criar relatório final")
    print("=" * 60)
    
    # Confirmar execução
    resposta = input("\n🤔 Deseja continuar? Isso pode demorar bastante tempo (s/N): ")
    if resposta.lower() not in ['s', 'sim', 'y', 'yes']:
        print("❌ Execução cancelada pelo usuário.")
        return 0
    
    success_count = 0
    
    # 1. Instalar dependências
    if install_dependencies():
        success_count += 1
    
    # 2. Executar benchmark (opcional se já existem resultados)
    if os.path.exists("resultados_geral_agrupado.csv"):
        print("\n📊 Arquivo de resultados já existe. Pulando benchmark...")
        resposta = input("🤔 Deseja executar benchmark novamente? (s/N): ")
        if resposta.lower() in ['s', 'sim', 'y', 'yes']:
            if run_full_benchmark():
                success_count += 1
        else:
            success_count += 1
    else:
        if run_full_benchmark():
            success_count += 1
    
    # 3. Gerar gráficos estáticos
    if generate_static_plots():
        success_count += 1
    
    # 4. Gerar gráficos interativos (opcional)
    generate_interactive_plots()  # Não conta como crítico
    
    # 5. Criar relatório final
    if create_final_report():
        success_count += 1
    
    # Resumo final
    print("\n" + "=" * 60)
    print("🎉 PROCESSO CONCLUÍDO!")
    print(f"✅ {success_count}/4 etapas críticas executadas com sucesso")
    
    if success_count >= 3:
        print("\n🎓 SEU TCC ESTÁ PRONTO!")
        print("📁 Verifique as pastas:")
        print("  📊 graficos_benchmark/ - Gráficos para inclusão no TCC")
        print("  🌐 graficos_interativos/ - Gráficos para apresentação")
        print("  📋 RELATORIO_FINAL.md - Relatório completo")
        print("\n💡 Dicas:")
        print("  - Use os arquivos .png nos gráficos estáticos no TCC")
        print("  - Use tabela_resumo.csv para criar tabelas no documento")
        print("  - Os arquivos .html são úteis para apresentações")
    else:
        print("\n⚠️  Algumas etapas falharam. Verifique os erros acima.")
    
    return 0 if success_count >= 3 else 1


if __name__ == "__main__":
    exit(main())