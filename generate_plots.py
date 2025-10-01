#!/usr/bin/env python3
"""
Script para gerar gráficos de análise dos resultados do benchmark de engines de ETL
Desenvolvido para uso em TCC - Comparação de Performance: DuckDB vs Pandas vs Polars
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
from pathlib import Path

# Configurações gerais para gráficos acadêmicos
plt.style.use('seaborn-v0_8-whitegrid')
sns.set_palette("husl")

# Configurações para alta qualidade (adequado para TCC)
plt.rcParams['figure.dpi'] = 300
plt.rcParams['savefig.dpi'] = 300
plt.rcParams['font.size'] = 12
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['axes.labelsize'] = 12
plt.rcParams['xtick.labelsize'] = 10
plt.rcParams['ytick.labelsize'] = 10
plt.rcParams['legend.fontsize'] = 11

# Cores consistentes para cada engine
CORES_ENGINES = {
    'duckdb': '#2E86AB',    # Azul
    'pandas': '#A23B72',    # Roxo/Rosa
    'polars': '#F18F01'     # Laranja
}

class BenchmarkVisualizer:
    """Classe para gerar visualizações dos resultados de benchmark"""
    
    def __init__(self, csv_path="resultados_geral_agrupado.csv"):
        """
        Inicializa o visualizador com os dados do CSV
        
        Args:
            csv_path (str): Caminho para o arquivo CSV com resultados agrupados
        """
        self.csv_path = csv_path
        self.df = self._load_and_clean_data()
        self.output_dir = Path("graficos_benchmark")
        self.output_dir.mkdir(exist_ok=True)
        
    def _load_and_clean_data(self):
        """Carrega e limpa os dados do CSV"""
        try:
            # Ler o CSV pulando a primeira linha (cabeçalho duplo)
            df = pd.read_csv(self.csv_path, header=[0, 1])
            
            # Renomear colunas para facilitar o uso
            df.columns = ['index', 'engine', 'operation', 'scenario', 
                         'time_mean', 'time_std', 'memory_mean', 'memory_std']
            
            # Remover a coluna index se existir
            if 'index' in df.columns:
                df = df.drop('index', axis=1)
                
            # Converter tipos
            numeric_cols = ['time_mean', 'time_std', 'memory_mean', 'memory_std']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            print(f"✅ Dados carregados: {len(df)} registros")
            print(f"📊 Engines: {sorted(df['engine'].unique())}")
            print(f"⚙️  Operações: {sorted(df['operation'].unique())}")
            print(f"📏 Cenários: {sorted(df['scenario'].unique())}")
            
            return df
            
        except Exception as e:
            print(f"❌ Erro ao carregar dados: {e}")
            raise
    
    def plot_tempo_por_engine(self):
        """Gráfico de barras: Tempo médio por engine e operação"""
        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        fig.suptitle('Tempo de Execução por Engine e Cenário', fontsize=16, y=1.02)
        
        scenarios = ['pequeno', 'medio', 'grande']
        
        for idx, scenario in enumerate(scenarios):
            ax = axes[idx]
            data = self.df[self.df['scenario'] == scenario]
            
            # Pivot para facilitar o plot
            pivot_data = data.pivot(index='operation', columns='engine', values='time_mean')
            
            # Plot com cores personalizadas
            bars = pivot_data.plot(kind='bar', ax=ax, color=[CORES_ENGINES[col] for col in pivot_data.columns])
            
            ax.set_title(f'Cenário {scenario.capitalize()}', fontweight='bold')
            ax.set_xlabel('Operações ETL')
            ax.set_ylabel('Tempo (segundos)')
            ax.tick_params(axis='x', rotation=45)
            ax.legend(title='Engine')
            ax.grid(True, alpha=0.3)
            
            # Adicionar valores nas barras para leitura clara
            for container in ax.containers:
                ax.bar_label(container, fmt='%.2f', rotation=90, fontsize=8)
        
        plt.tight_layout()
        self._save_plot('tempo_por_engine_cenario.png')
        return fig
    
    def plot_memoria_por_engine(self):
        """Gráfico de barras: Uso de memória por engine e operação"""
        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        fig.suptitle('Uso de Memória por Engine e Cenário', fontsize=16, y=1.02)
        
        scenarios = ['pequeno', 'medio', 'grande']
        
        for idx, scenario in enumerate(scenarios):
            ax = axes[idx]
            data = self.df[self.df['scenario'] == scenario]
            
            # Pivot para facilitar o plot
            pivot_data = data.pivot(index='operation', columns='engine', values='memory_mean')
            
            # Plot com cores personalizadas
            bars = pivot_data.plot(kind='bar', ax=ax, color=[CORES_ENGINES[col] for col in pivot_data.columns])
            
            ax.set_title(f'Cenário {scenario.capitalize()}', fontweight='bold')
            ax.set_xlabel('Operações ETL')
            ax.set_ylabel('Memória (MB)')
            ax.tick_params(axis='x', rotation=45)
            ax.legend(title='Engine')
            ax.grid(True, alpha=0.3)
            
            # Adicionar valores nas barras
            for container in ax.containers:
                ax.bar_label(container, fmt='%.1f', rotation=90, fontsize=8)
        
        plt.tight_layout()
        self._save_plot('memoria_por_engine_cenario.png')
        return fig
    
    def plot_escalabilidade_tempo(self):
        """Gráfico de linhas: Escalabilidade do tempo por tamanho do dataset"""
        operations = self.df['operation'].unique()
        
        fig, axes = plt.subplots(2, 4, figsize=(20, 10))
        fig.suptitle('Análise de Escalabilidade - Tempo de Execução', fontsize=16, y=0.98)
        
        axes = axes.flatten()
        
        for idx, operation in enumerate(operations):
            if idx >= len(axes):
                break
                
            ax = axes[idx]
            data = self.df[self.df['operation'] == operation]
            
            scenario_order = ['pequeno', 'medio', 'grande']
            
            for engine in data['engine'].unique():
                engine_data = data[data['engine'] == engine]
                
                # Ordenar por cenário
                engine_data = engine_data.set_index('scenario').reindex(scenario_order)
                
                ax.plot(scenario_order, engine_data['time_mean'], 
                       marker='o', linewidth=2.5, markersize=8,
                       label=engine.capitalize(), color=CORES_ENGINES[engine])
                
                # Adicionar barras de erro
                ax.errorbar(scenario_order, engine_data['time_mean'], 
                           yerr=engine_data['time_std'], 
                           color=CORES_ENGINES[engine], alpha=0.3, capsize=5)
            
            ax.set_title(f'{operation.replace("_", " ").title()}', fontweight='bold')
            ax.set_xlabel('Tamanho do Dataset')
            ax.set_ylabel('Tempo (segundos)')
            ax.legend()
            ax.grid(True, alpha=0.3)
            ax.set_yscale('log')  # Escala log para melhor visualização
        
        # Remover subplots vazios
        for idx in range(len(operations), len(axes)):
            fig.delaxes(axes[idx])
        
        plt.tight_layout()
        self._save_plot('escalabilidade_tempo.png')
        return fig
    
    def plot_escalabilidade_memoria(self):
        """Gráfico de linhas: Escalabilidade da memória por tamanho do dataset"""
        operations = self.df['operation'].unique()
        
        fig, axes = plt.subplots(2, 4, figsize=(20, 10))
        fig.suptitle('Análise de Escalabilidade - Uso de Memória', fontsize=16, y=0.98)
        
        axes = axes.flatten()
        
        for idx, operation in enumerate(operations):
            if idx >= len(axes):
                break
                
            ax = axes[idx]
            data = self.df[self.df['operation'] == operation]
            
            scenario_order = ['pequeno', 'medio', 'grande']
            
            for engine in data['engine'].unique():
                engine_data = data[data['engine'] == engine]
                
                # Ordenar por cenário
                engine_data = engine_data.set_index('scenario').reindex(scenario_order)
                
                ax.plot(scenario_order, engine_data['memory_mean'], 
                       marker='s', linewidth=2.5, markersize=8,
                       label=engine.capitalize(), color=CORES_ENGINES[engine])
                
                # Adicionar barras de erro
                ax.errorbar(scenario_order, engine_data['memory_mean'], 
                           yerr=engine_data['memory_std'], 
                           color=CORES_ENGINES[engine], alpha=0.3, capsize=5)
            
            ax.set_title(f'{operation.replace("_", " ").title()}', fontweight='bold')
            ax.set_xlabel('Tamanho do Dataset')
            ax.set_ylabel('Memória (MB)')
            ax.legend()
            ax.grid(True, alpha=0.3)
            ax.set_yscale('log')  # Escala log para melhor visualização
        
        # Remover subplots vazios
        for idx in range(len(operations), len(axes)):
            fig.delaxes(axes[idx])
        
        plt.tight_layout()
        self._save_plot('escalabilidade_memoria.png')
        return fig
    
    def plot_heatmap_performance(self):
        """Heatmap da performance relativa dos engines"""
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
        
        # Heatmap para tempo
        pivot_time = self.df.groupby(['engine', 'operation'])['time_mean'].mean().unstack()
        sns.heatmap(pivot_time, annot=True, fmt='.2f', cmap='YlOrRd', ax=ax1)
        ax1.set_title('Tempo Médio por Engine e Operação (segundos)', fontweight='bold')
        ax1.set_xlabel('Operações ETL')
        ax1.set_ylabel('Engine')
        
        # Heatmap para memória
        pivot_memory = self.df.groupby(['engine', 'operation'])['memory_mean'].mean().unstack()
        sns.heatmap(pivot_memory, annot=True, fmt='.1f', cmap='YlGnBu', ax=ax2)
        ax2.set_title('Uso Médio de Memória por Engine e Operação (MB)', fontweight='bold')
        ax2.set_xlabel('Operações ETL')
        ax2.set_ylabel('Engine')
        
        plt.tight_layout()
        self._save_plot('heatmap_performance.png')
        return fig
    
    def plot_radar_chart_engines(self):
        """Gráfico radar comparando engines em diferentes métricas"""
        # Normalizar métricas para 0-1 (menor = melhor, então invertemos)
        metrics_data = []
        
        for engine in self.df['engine'].unique():
            engine_data = self.df[self.df['engine'] == engine]
            
            # Calcular médias por operação
            avg_time = engine_data.groupby('operation')['time_mean'].mean().mean()
            avg_memory = engine_data.groupby('operation')['memory_mean'].mean().mean()
            
            # Métricas específicas (inverter para que maior = melhor)
            read_speed = 1 / engine_data[engine_data['operation'].str.contains('read')]['time_mean'].mean()
            write_speed = 1 / engine_data[engine_data['operation'].str.contains('write')]['time_mean'].mean()
            memory_efficiency = 1 / avg_memory if avg_memory > 0 else 1
            
            metrics_data.append({
                'engine': engine,
                'Velocidade Leitura': read_speed,
                'Velocidade Escrita': write_speed,
                'Eficiência Memória': memory_efficiency,
                'Performance Geral': 1 / avg_time if avg_time > 0 else 1
            })
        
        # Normalizar todas as métricas para 0-1
        df_metrics = pd.DataFrame(metrics_data)
        metrics_cols = ['Velocidade Leitura', 'Velocidade Escrita', 'Eficiência Memória', 'Performance Geral']
        
        for col in metrics_cols:
            df_metrics[col] = (df_metrics[col] - df_metrics[col].min()) / (df_metrics[col].max() - df_metrics[col].min())
        
        # Criar gráfico radar
        fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(projection='polar'))
        
        angles = np.linspace(0, 2 * np.pi, len(metrics_cols), endpoint=False)
        angles = np.concatenate((angles, [angles[0]]))  # Fechar o círculo
        
        for idx, row in df_metrics.iterrows():
            values = row[metrics_cols].values
            values = np.concatenate((values, [values[0]]))  # Fechar o círculo
            
            ax.plot(angles, values, 'o-', linewidth=2, 
                   label=row['engine'].capitalize(), 
                   color=CORES_ENGINES[row['engine']])
            ax.fill(angles, values, alpha=0.25, color=CORES_ENGINES[row['engine']])
        
        ax.set_xticks(angles[:-1])
        ax.set_xticklabels(metrics_cols)
        ax.set_ylim(0, 1)
        ax.set_title('Comparação Multidimensional de Performance', size=16, fontweight='bold', pad=20)
        ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.0))
        ax.grid(True)
        
        plt.tight_layout()
        self._save_plot('radar_chart_engines.png')
        return fig
    
    def plot_tempo_vs_memoria(self):
        """Scatter plot: Tempo vs Memória para análise de trade-offs"""
        fig, axes = plt.subplots(1, 3, figsize=(18, 6))
        fig.suptitle('Trade-off Tempo vs Memória por Cenário', fontsize=16, y=1.02)
        
        scenarios = ['pequeno', 'medio', 'grande']
        
        for idx, scenario in enumerate(scenarios):
            ax = axes[idx]
            data = self.df[self.df['scenario'] == scenario]
            
            for engine in data['engine'].unique():
                engine_data = data[data['engine'] == engine]
                
                scatter = ax.scatter(engine_data['time_mean'], engine_data['memory_mean'],
                                   label=engine.capitalize(), 
                                   color=CORES_ENGINES[engine],
                                   s=100, alpha=0.7, edgecolors='black', linewidth=0.5)
                
                # Adicionar labels das operações
                for _, point in engine_data.iterrows():
                    ax.annotate(point['operation'], 
                              (point['time_mean'], point['memory_mean']),
                              xytext=(5, 5), textcoords='offset points',
                              fontsize=8, alpha=0.8)
            
            ax.set_title(f'Cenário {scenario.capitalize()}', fontweight='bold')
            ax.set_xlabel('Tempo (segundos)')
            ax.set_ylabel('Memória (MB)')
            ax.legend()
            ax.grid(True, alpha=0.3)
            ax.set_xscale('log')
            ax.set_yscale('log')
        
        plt.tight_layout()
        self._save_plot('tempo_vs_memoria.png')
        return fig
    
    def generate_summary_table(self):
        """Gera tabela resumo para inclusão no TCC"""
        summary = self.df.groupby(['engine', 'scenario']).agg({
            'time_mean': ['mean', 'std'],
            'memory_mean': ['mean', 'std']
        }).round(3)
        
        # Achatar colunas
        summary.columns = ['_'.join(col).strip() for col in summary.columns]
        summary = summary.reset_index()
        
        # Salvar como CSV
        summary_path = self.output_dir / 'tabela_resumo.csv'
        summary.to_csv(summary_path, index=False)
        print(f"📊 Tabela resumo salva em: {summary_path}")
        
        return summary
    
    def _save_plot(self, filename):
        """Salva o gráfico com alta qualidade"""
        filepath = self.output_dir / filename
        plt.savefig(filepath, dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        print(f"📈 Gráfico salvo: {filepath}")
    
    def generate_all_plots(self):
        """Gera todos os gráficos para o TCC"""
        print("🎨 Gerando visualizações para TCC...")
        print("=" * 50)
        
        try:
            # Gerar todos os gráficos
            self.plot_tempo_por_engine()
            plt.close()
            
            self.plot_memoria_por_engine()
            plt.close()
            
            self.plot_escalabilidade_tempo()
            plt.close()
            
            self.plot_escalabilidade_memoria()
            plt.close()
            
            self.plot_heatmap_performance()
            plt.close()
            
            self.plot_radar_chart_engines()
            plt.close()
            
            self.plot_tempo_vs_memoria()
            plt.close()
            
            # Gerar tabela resumo
            self.generate_summary_table()
            
            print("\n🎉 Todos os gráficos foram gerados com sucesso!")
            print(f"📁 Arquivos salvos em: {self.output_dir.absolute()}")
            print("\n📋 Lista de arquivos gerados:")
            for file in sorted(self.output_dir.glob("*.png")):
                print(f"  📈 {file.name}")
            for file in sorted(self.output_dir.glob("*.csv")):
                print(f"  📊 {file.name}")
                
        except Exception as e:
            print(f"❌ Erro durante a geração: {e}")
            raise


def main():
    """Função principal"""
    print("🚀 Iniciando geração de gráficos para TCC - Benchmark ETL Engines")
    print("=" * 60)
    
    # Verificar se o arquivo existe
    csv_file = "resultados_geral_agrupado.csv"
    if not os.path.exists(csv_file):
        print(f"❌ Arquivo não encontrado: {csv_file}")
        print("💡 Execute primeiro o benchmark para gerar os resultados.")
        return
    
    # Criar visualizador e gerar gráficos
    try:
        visualizer = BenchmarkVisualizer(csv_file)
        visualizer.generate_all_plots()
        
        print("\n✅ Processo concluído!")
        print("🎓 Gráficos prontos para uso no TCC!")
        
    except Exception as e:
        print(f"❌ Erro na execução: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())