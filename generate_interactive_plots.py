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
    
    def create_individual_interactive_plots(self):
        """Cria gráficos interativos individuais ao invés de dashboard único"""
        
        # 1. Gráfico de tempo por engine - individual para cada cenário
        scenarios = ['pequeno', 'medio', 'grande']
        
        for scenario in scenarios:
            data = self.df[self.df['scenario'] == scenario]
            
            fig = go.Figure()
            
            for engine in data['engine'].unique():
                engine_data = data[data['engine'] == engine]
                
                fig.add_trace(go.Bar(
                    x=engine_data['operation'],
                    y=engine_data['time_mean'],
                    name=engine.capitalize(),
                    marker_color=self.cores_engines[engine],
                    error_y=dict(type='data', array=engine_data['time_std'])
                ))
            
            fig.update_layout(
                title=f"Tempo de Execução - Cenário {scenario.capitalize()}",
                xaxis_title="Operações ETL",
                yaxis_title="Tempo (segundos)",
                height=600,
                showlegend=True,
                barmode='group'
            )
            
            output_path = self.output_dir / f"tempo_por_engine_{scenario}.html"
            pyo.plot(fig, filename=str(output_path), auto_open=False)
            print(f"📊 Gráfico tempo {scenario} salvo: {output_path.name}")
        
        # 2. Gráfico de memória por engine - individual para cada cenário
        for scenario in scenarios:
            data = self.df[self.df['scenario'] == scenario]
            
            fig = go.Figure()
            
            for engine in data['engine'].unique():
                engine_data = data[data['engine'] == engine]
                
                fig.add_trace(go.Bar(
                    x=engine_data['operation'],
                    y=engine_data['memory_mean'],
                    name=engine.capitalize(),
                    marker_color=self.cores_engines[engine],
                    error_y=dict(type='data', array=engine_data['memory_std'])
                ))
            
            fig.update_layout(
                title=f"Uso de Memória - Cenário {scenario.capitalize()}",
                xaxis_title="Operações ETL",
                yaxis_title="Memória (MB)",
                height=600,
                showlegend=True,
                barmode='group'
            )
            
            output_path = self.output_dir / f"memoria_por_engine_{scenario}.html"
            pyo.plot(fig, filename=str(output_path), auto_open=False)
            print(f"📊 Gráfico memória {scenario} salvo: {output_path.name}")
        
        # 3. Gráficos de escalabilidade individuais por operação
        operations = self.df['operation'].unique()
        
        for operation in operations:
            # Escalabilidade de tempo
            fig_time = go.Figure()
            data = self.df[self.df['operation'] == operation]
            
            scenario_order = ['pequeno', 'medio', 'grande']
            
            for engine in data['engine'].unique():
                engine_data = data[data['engine'] == engine]
                engine_data = engine_data.set_index('scenario').reindex(scenario_order)
                
                fig_time.add_trace(go.Scatter(
                    x=scenario_order,
                    y=engine_data['time_mean'],
                    mode='lines+markers',
                    name=engine.capitalize(),
                    line=dict(color=self.cores_engines[engine], width=3),
                    marker=dict(size=10),
                    error_y=dict(type='data', array=engine_data['time_std'])
                ))
            
            fig_time.update_layout(
                title=f"Escalabilidade de Tempo - {operation.replace('_', ' ').title()}",
                xaxis_title="Tamanho do Dataset",
                yaxis_title="Tempo (segundos)",
                yaxis_type="log",
                height=600,
                showlegend=True
            )
            
            output_path = self.output_dir / f"escalabilidade_tempo_{operation}.html"
            pyo.plot(fig_time, filename=str(output_path), auto_open=False)
            print(f"📊 Escalabilidade tempo {operation} salva: {output_path.name}")
            
            # Escalabilidade de memória
            fig_memory = go.Figure()
            
            for engine in data['engine'].unique():
                engine_data = data[data['engine'] == engine]
                engine_data = engine_data.set_index('scenario').reindex(scenario_order)
                
                fig_memory.add_trace(go.Scatter(
                    x=scenario_order,
                    y=engine_data['memory_mean'],
                    mode='lines+markers',
                    name=engine.capitalize(),
                    line=dict(color=self.cores_engines[engine], width=3),
                    marker=dict(size=10, symbol='square'),
                    error_y=dict(type='data', array=engine_data['memory_std'])
                ))
            
            fig_memory.update_layout(
                title=f"Escalabilidade de Memória - {operation.replace('_', ' ').title()}",
                xaxis_title="Tamanho do Dataset",
                yaxis_title="Memória (MB)",
                yaxis_type="log",
                height=600,
                showlegend=True
            )
            
            output_path = self.output_dir / f"escalabilidade_memoria_{operation}.html"
            pyo.plot(fig_memory, filename=str(output_path), auto_open=False)
            print(f"📊 Escalabilidade memória {operation} salva: {output_path.name}")
        
        return True
    
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
            self.create_individual_interactive_plots()
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