#!/usr/bin/env python3
"""
Script complementar para gerar gráficos interativos usando Plotly
Ideal para análise exploratória e apresentações interativas
"""

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import plotly.offline as pyo
from pathlib import Path
import os

class InteractiveBenchmarkVisualizer:
    """Classe para gerar visualizações interativas dos resultados de benchmark"""
    
    def __init__(self, csv_path="resultados_geral_agrupado.csv"):
        self.csv_path = csv_path
        self.df = self._load_and_clean_data()
        self.output_dir = Path("graficos_interativos")
        self.output_dir.mkdir(exist_ok=True)
        
        # Cores consistentes
        self.cores_engines = {
            'duckdb': '#2E86AB',
            'pandas': '#A23B72', 
            'polars': '#F18F01'
        }
    
    def _load_and_clean_data(self):
        """Carrega e limpa os dados do CSV"""
        try:
            df = pd.read_csv(self.csv_path, header=[0, 1])
            df.columns = ['index', 'engine', 'operation', 'scenario', 
                         'time_mean', 'time_std', 'memory_mean', 'memory_std']
            
            if 'index' in df.columns:
                df = df.drop('index', axis=1)
                
            numeric_cols = ['time_mean', 'time_std', 'memory_mean', 'memory_std']
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            return df
            
        except Exception as e:
            print(f"❌ Erro ao carregar dados: {e}")
            raise
    
    def create_interactive_dashboard(self):
        """Cria um dashboard interativo completo"""
        
        # Criar subplots
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('Tempo de Execução por Engine', 'Uso de Memória por Engine',
                          'Escalabilidade - Tempo', 'Escalabilidade - Memória'),
            specs=[[{"secondary_y": False}, {"secondary_y": False}],
                   [{"secondary_y": False}, {"secondary_y": False}]]
        )
        
        # Gráfico 1: Tempo por engine (boxplot)
        for engine in self.df['engine'].unique():
            engine_data = self.df[self.df['engine'] == engine]
            fig.add_trace(
                go.Box(y=engine_data['time_mean'], name=f'{engine.capitalize()} - Tempo',
                      marker_color=self.cores_engines[engine],
                      showlegend=True),
                row=1, col=1
            )
        
        # Gráfico 2: Memória por engine (boxplot)
        for engine in self.df['engine'].unique():
            engine_data = self.df[self.df['engine'] == engine]
            fig.add_trace(
                go.Box(y=engine_data['memory_mean'], name=f'{engine.capitalize()} - Memória',
                      marker_color=self.cores_engines[engine],
                      showlegend=False),
                row=1, col=2
            )
        
        # Gráfico 3: Escalabilidade tempo
        scenarios = ['pequeno', 'medio', 'grande']
        for engine in self.df['engine'].unique():
            engine_scenarios = []
            engine_times = []
            for scenario in scenarios:
                data = self.df[(self.df['engine'] == engine) & (self.df['scenario'] == scenario)]
                if not data.empty:
                    engine_scenarios.append(scenario)
                    engine_times.append(data['time_mean'].mean())
            
            fig.add_trace(
                go.Scatter(x=engine_scenarios, y=engine_times, 
                          name=f'{engine.capitalize()} - Tempo Escala',
                          mode='lines+markers',
                          line=dict(color=self.cores_engines[engine]),
                          showlegend=False),
                row=2, col=1
            )
        
        # Gráfico 4: Escalabilidade memória
        for engine in self.df['engine'].unique():
            engine_scenarios = []
            engine_memory = []
            for scenario in scenarios:
                data = self.df[(self.df['engine'] == engine) & (self.df['scenario'] == scenario)]
                if not data.empty:
                    engine_scenarios.append(scenario)
                    engine_memory.append(data['memory_mean'].mean())
            
            fig.add_trace(
                go.Scatter(x=engine_scenarios, y=engine_memory,
                          name=f'{engine.capitalize()} - Memória Escala',
                          mode='lines+markers',
                          line=dict(color=self.cores_engines[engine]),
                          showlegend=False),
                row=2, col=2
            )
        
        # Atualizar layout
        fig.update_layout(
            title="Dashboard Interativo - Benchmark ETL Engines",
            height=800,
            showlegend=True
        )
        
        # Salvar
        output_path = self.output_dir / "dashboard_interativo.html"
        pyo.plot(fig, filename=str(output_path), auto_open=False)
        print(f"📊 Dashboard interativo salvo: {output_path}")
        
        return fig
    
    def create_3d_performance_plot(self):
        """Cria gráfico 3D de performance (Tempo vs Memória vs Cenário)"""
        
        # Mapear cenários para valores numéricos
        scenario_map = {'pequeno': 1, 'medio': 2, 'grande': 3}
        self.df['scenario_num'] = self.df['scenario'].map(scenario_map)
        
        fig = go.Figure()
        
        for engine in self.df['engine'].unique():
            engine_data = self.df[self.df['engine'] == engine]
            
            fig.add_trace(go.Scatter3d(
                x=engine_data['time_mean'],
                y=engine_data['memory_mean'],
                z=engine_data['scenario_num'],
                mode='markers+text',
                marker=dict(
                    size=8,
                    color=self.cores_engines[engine],
                    symbol='circle'
                ),
                text=engine_data['operation'],
                textposition="middle center",
                name=engine.capitalize(),
                hovertemplate=
                '<b>%{fullData.name}</b><br>' +
                'Operação: %{text}<br>' +
                'Tempo: %{x:.3f}s<br>' +
                'Memória: %{y:.1f}MB<br>' +
                'Cenário: %{z}<br>' +
                '<extra></extra>'
            ))
        
        fig.update_layout(
            title="Análise 3D: Tempo vs Memória vs Cenário",
            scene=dict(
                xaxis_title="Tempo (segundos)",
                yaxis_title="Memória (MB)",
                zaxis_title="Cenário (1=Pequeno, 2=Médio, 3=Grande)",
                camera=dict(eye=dict(x=1.2, y=1.2, z=1.2))
            ),
            height=700
        )
        
        output_path = self.output_dir / "analise_3d.html"
        pyo.plot(fig, filename=str(output_path), auto_open=False)
        print(f"📊 Gráfico 3D salvo: {output_path}")
        
        return fig
    
    def create_sunburst_chart(self):
        """Cria gráfico sunburst para visualizar hierarquia dos dados"""
        
        # Preparar dados para sunburst
        df_sunburst = self.df.copy()
        df_sunburst['path'] = (df_sunburst['engine'] + ' / ' + 
                              df_sunburst['scenario'] + ' / ' + 
                              df_sunburst['operation'])
        
        fig = go.Figure(go.Sunburst(
            labels=df_sunburst['path'].str.split(' / ').explode().unique(),
            parents=[''] * len(df_sunburst['engine'].unique()) + 
                    df_sunburst['engine'].tolist() * len(df_sunburst['scenario'].unique()) +
                    (df_sunburst['engine'] + ' / ' + df_sunburst['scenario']).tolist(),
            values=df_sunburst['time_mean'].tolist() * 3,  # Repetir para hierarquia
            branchvalues="total",
            hovertemplate='<b>%{label}</b><br>Tempo: %{value:.3f}s<extra></extra>',
            maxdepth=3
        ))
        
        fig.update_layout(
            title="Distribuição Hierárquica do Tempo de Execução",
            height=600
        )
        
        output_path = self.output_dir / "sunburst_tempo.html"
        pyo.plot(fig, filename=str(output_path), auto_open=False)
        print(f"📊 Gráfico sunburst salvo: {output_path}")
        
        return fig
    
    def create_animated_performance(self):
        """Cria gráfico animado mostrando evolução entre cenários"""
        
        # Preparar dados para animação
        df_anim = self.df.copy()
        
        fig = px.scatter(
            df_anim,
            x="time_mean",
            y="memory_mean", 
            animation_frame="scenario",
            animation_group="operation",
            color="engine",
            size="time_mean",
            hover_name="operation",
            color_discrete_map=self.cores_engines,
            title="Evolução da Performance por Cenário (Animado)",
            labels={
                "time_mean": "Tempo (segundos)",
                "memory_mean": "Memória (MB)",
                "engine": "Engine"
            }
        )
        
        fig.update_layout(height=600)
        
        output_path = self.output_dir / "animacao_performance.html"
        pyo.plot(fig, filename=str(output_path), auto_open=False)
        print(f"📊 Animação salva: {output_path}")
        
        return fig
    
    def generate_all_interactive_plots(self):
        """Gera todas as visualizações interativas"""
        print("🎨 Gerando visualizações interativas...")
        print("=" * 50)
        
        try:
            self.create_interactive_dashboard()
            self.create_3d_performance_plot()
            self.create_animated_performance()
            
            print("\n🎉 Visualizações interativas geradas com sucesso!")
            print(f"📁 Arquivos salvos em: {self.output_dir.absolute()}")
            print("\n📋 Lista de arquivos gerados:")
            for file in sorted(self.output_dir.glob("*.html")):
                print(f"  🌐 {file.name}")
                
        except Exception as e:
            print(f"❌ Erro durante a geração: {e}")
            raise


def main():
    """Função principal para gráficos interativos"""
    print("🚀 Iniciando geração de gráficos interativos")
    print("=" * 50)
    
    csv_file = "resultados_geral_agrupado.csv"
    if not os.path.exists(csv_file):
        print(f"❌ Arquivo não encontrado: {csv_file}")
        return 1
    
    try:
        visualizer = InteractiveBenchmarkVisualizer(csv_file)
        visualizer.generate_all_interactive_plots()
        
        print("\n✅ Processo concluído!")
        print("🌐 Abra os arquivos .html no navegador para visualização interativa!")
        
    except Exception as e:
        print(f"❌ Erro na execução: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())