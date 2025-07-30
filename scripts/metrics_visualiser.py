import json
from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.subplots as sp
import plotly.graph_objects as go
import plotly.io as pio


def flatten_endpoints(data):
    """Flatten the nested 'endpoints' dict in each record."""
    for record in data:
        endpoints = record.pop('endpoints', {})
        for endpoint, metrics in endpoints.items():
            for metric_key, metric_val in metrics.items():
                col_name = f"{endpoint}_{metric_key}"
                record[col_name] = metric_val
    return data


def plot_metrics_from_log(filedir, filename):
    filepath = filedir / filename
    data = []
    with open(filepath, 'r') as f:
        for line in f:
            data.append(json.loads(line))

    # Flatten nested endpoints dict
    data = flatten_endpoints(data)

    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')

    # Exclude timestamp from metric columns
    metrics = [col for col in df.columns if col != 'timestamp']
    num_metrics = len(metrics)

    fig = sp.make_subplots(rows=num_metrics, cols=1, shared_xaxes=True,
                           subplot_titles=metrics)

    for i, metric in enumerate(metrics, start=1):
        fig.add_trace(
            go.Scatter(x=df['timestamp'], y=df[metric],
                       mode='lines+markers', name=metric),
            row=i, col=1
        )

    fig.update_layout(height=300*num_metrics, width=900,
                      title_text=f"Metrics Over Time: {filename}")
    fig.update_xaxes(title_text="Timestamp", row=num_metrics, col=1)

    pio.write_html(
        fig, f"scripts/graphs/{filename}-stats.html", auto_open=True)


def plot_consumer_metrics_from_log(filedir, filename):
    filepath = filedir / filename
    data = []
    with open(filepath, 'r') as f:
        for line in f:
            data.append(json.loads(line))

    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')

    if 'operator_name' not in df.columns:
        raise ValueError("Missing 'operator_name' in log data")

    operator_names = df['operator_name'].unique()

    for operator in operator_names:
        operator_df = df[df['operator_name'] == operator]
        metrics = [col for col in operator_df.columns
                   if col not in ['timestamp', 'operator_name']]

        num_metrics = len(metrics)
        fig = sp.make_subplots(rows=num_metrics, cols=1, shared_xaxes=True,
                               subplot_titles=metrics)

        for i, metric in enumerate(metrics, start=1):
            fig.add_trace(
                go.Scatter(
                    x=operator_df['timestamp'],
                    y=operator_df[metric],
                    mode='lines+markers',
                    name=metric
                ),
                row=i, col=1
            )

        fig.update_layout(height=300*num_metrics, width=900,
                          title_text=f"Metrics Over Time: {operator} ({filename})")
        fig.update_xaxes(title_text="Timestamp", row=num_metrics, col=1)

        output_path = f"scripts/graphs/{filename}-{operator}-stats.html"
        pio.write_html(fig, output_path, auto_open=True)


if __name__ == "__main__":
    current_file = Path(__file__).resolve()
    scripts_dir = current_file.parent
    log_dir = scripts_dir.parent / "metrics"
    plot_metrics_from_log(log_dir, "kafka-producer.log")
    plot_metrics_from_log(log_dir, "flask-backend.log")
    plot_consumer_metrics_from_log(log_dir, "flink-consumer.log")
