# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": { 
# MAGIC         "name": "Audit",
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import mlflow
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd
from matplotlib.dates import DateFormatter
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from scipy import stats
from scipy.stats import entropy, ks_2samp, mannwhitneyu, ttest_ind
from sklearn.covariance import EllipticEnvelope
from sklearn.neighbors import LocalOutlierFactor


@dataclass
class DriftAlert:
    metric_name: str
    current_value: float
    reference_value: float
    drift_score: float
    drift_type: str  # 'sudden', 'gradual', 'trend', or 'anomaly'
    severity: str
    experiment_name: str
    timestamp: datetime
    run_id: str
    window_size: int
    statistical_test: str
    current_dataset_version: str
    current_dataset_run_id: str
    reference_dataset_version: str
    reference_dataset_run_id: str

    def __str__(self) -> str:
        """Return a human-readable string representation of the alert"""
        change_pct = ((self.current_value - self.reference_value) / self.reference_value) * 100
        return (
            f"DriftAlert[{self.severity}] - {self.metric_name}\n"
            f"  Type: {self.drift_type}\n"# (using {self.statistical_test})\n"
            f"  Current: {self.current_value:.4f} (vs {self.reference_value:.4f}, {change_pct:+.1f}%)\n"
            f"  Score: {self.drift_score:.4f}\n"
            f"  Dataset: {self.current_dataset_version} (current) vs {self.reference_dataset_version} (reference)\n"
            f"  Time: {self.timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
        )

    @classmethod
    def create(cls, 
               metric_name: str,
               current_run: Dict,
               reference_run: Dict,
               drift_score: float,
               drift_type: str,
               severity: str,
               experiment_name: str,
               window_size: int,
               statistical_test: str
               ) -> 'DriftAlert':
        """Factory method to create a DriftAlert with common parameters"""
        return cls(
            metric_name=metric_name,
            current_value=current_run['metrics'][metric_name],
            reference_value=reference_run['metrics'][metric_name],
            drift_score=drift_score,
            drift_type=drift_type,
            severity=severity,
            experiment_name=experiment_name,
            timestamp=datetime.now(),
            run_id=current_run['run_id'],
            window_size=window_size,
            statistical_test=statistical_test,
            current_dataset_version=current_run['dataset_version'],
            current_dataset_run_id=current_run['dataset_run_id'],
            reference_dataset_version=reference_run['dataset_version'],
            reference_dataset_run_id=reference_run['dataset_run_id']
        )

    def to_dict(self) -> Dict:
        """Convert the alert to a dictionary"""
        return asdict(self)

    def to_json(self) -> str:
        """Convert the alert to a JSON string"""
        import json
        return json.dumps(self.to_dict(), default=str)

class ModelDriftMonitor:
    def __init__(
        self,
        experiment_name: str,
        run_name: str = None,
        drift_threshold: float = 0.1,
        window_size: int = 5,
        metrics_to_monitor: Optional[List[str]] = None,
        statistical_tests: Optional[List[str]] = None,
        drift_types: Optional[List[str]] = None,
        contamination: float = 0.1,  # Expected proportion of outliers
        p_value_threshold: float = 0.05,  # Threshold for statistical significance
    ):
        self.experiment_name = experiment_name
        self.run_name = run_name
        self.drift_threshold = drift_threshold
        self.window_size = window_size
        self.contamination = contamination
        self.p_value_threshold = p_value_threshold
        self.metrics_to_monitor = metrics_to_monitor or [
            'MAP_k1', 'MAP_k5', 'MAP_k10',
            'NDCG_k1', 'NDCG_k5', 'NDCG_k10',
            'mae', 'rmse'
        ]
        self.statistical_tests = statistical_tests or ['ks_test', 'kl_divergence']
        self.drift_types = drift_types or ['sudden', 'gradual', 'trend', 'anomaly']
        self.spark = SparkSession.builder.getOrCreate()
        
        # Define metric types and their expected drift direction
        self.metric_types = {
            'higher_better': ['MAP_k1', 'MAP_k5', 'MAP_k10', 'NDCG_k1', 'NDCG_k5', 'NDCG_k10', 
                'avg_interactions_per_user', 'avg_interactions_per_item',
                'catalog_coverage_percent', 'recommendation_gini'
                ],
            'lower_better': ['mae', 'rmse']
        }

    def __str__(self) -> str:
        """Return a human-readable string representation of the monitor"""
        return (
            f"ModelDriftMonitor for experiment '{self.experiment_name}{'-'+self.run_name if self.run_name else ''}'\n"
            f"  Monitoring {len(self.metrics_to_monitor)} metrics:\n"
            f"    {', '.join(self.metrics_to_monitor)}\n"
            f"  Configuration:\n"
            f"    - Drift threshold: {self.drift_threshold:.2f}\n"
            f"    - Window size: {self.window_size} runs\n"
            f"    - Contamination: {self.contamination:.2f}\n"
            f"    - Statistical tests: {', '.join(self.statistical_tests)}\n"
            f"    - Drift types: {', '.join(self.drift_types)}"
        )

    def get_recent_runs(self, n_runs: int = None) -> List[Dict]:
        """Retrieve recent successful runs from MLflow"""
        n_runs = n_runs or self.window_size
        client = mlflow.tracking.MlflowClient()
        experiment = client.get_experiment_by_name(self.experiment_name)
        filter_str = "attributes.status = 'Finished'"
        if self.run_name is not None:
            filter_str += f" AND tags.mlflow.runName = '{self.run_name}'"

        runs = client.search_runs(
            experiment_ids=[experiment.experiment_id],
            filter_string=filter_str,
            order_by=["start_time DESC"],
            max_results=n_runs
        )
        
        return [{
            'run_id': run.info.run_id,
            'metrics': run.data.metrics,
            'timestamp': datetime.fromtimestamp(run.info.start_time / 1000.0),
            'tags': run.data.tags,
            'dataset_version': run.data.tags.get('dataset_version', 'unknown'),
            'dataset_run_id': run.data.tags.get('dataset_run_id', 'unknown')
        } for run in runs]

    def get_existing_alerts(self, metric_name: str, drift_type: str, window_size: int) -> List[Dict]:
        """Get existing alerts for a metric and drift type within the window size"""
        query = f"""
        SELECT *
        FROM Audit.model_drift_alerts
        WHERE metric_name = '{metric_name}'
        AND drift_type = '{drift_type}'
        AND timestamp >= date_sub(current_timestamp(), {window_size})
        ORDER BY timestamp DESC
        """
        return self.spark.sql(query).collect()


    def is_metric_higher_better(self, metric_name: str) -> bool:
        """Determine if higher values are better for a metric"""
        for metric_type, metrics in self.metric_types.items():
            if metric_name in metrics:
                return metric_type == 'higher_better'
        return True  # Default to higher is better if unknown


    def calculate_statistical_tests(self, current_value: float, reference_value: float) -> Dict[str, Tuple[float, float]]:
        """Calculate multiple statistical tests and their p-values"""
        results = {}

        if "abs_diff" in self.statistical_tests:
            relative_diff = current_value - reference_value
            results["abs_diff"] = (relative_diff, None)
        if "rel_change" in self.statistical_tests:
            relative_change = (current_value - reference_value) / reference_value
            results["rel_change"] = (relative_change, None)

        
        return results

    def detect_sudden_drift(self, current_run: Dict, previous_run: Dict) -> List[Tuple[str, float, str, str]]:
        """Detect sudden changes in metrics using statistical tests"""
        alerts = []
        for metric in self.metrics_to_monitor:
            if metric not in current_run['metrics'] or metric not in previous_run['metrics']:
                continue
                
            current_value = current_run['metrics'][metric]
            previous_value = previous_run['metrics'][metric]
            
            # Prepare data for statistical tests
            current_value = current_value
            reference_value = previous_value
            
            # Calculate statistical tests
            test_results = self.calculate_statistical_tests(current_value, reference_value)

            higher_better = self.is_metric_higher_better(metric)
            
            # shortcut - only care about abs_drift
            abs_drift = [(test, score) for test, (score, _) 
                                    in test_results.items() 
                                if test == "abs_diff" and 
                                    ((score <= -self.drift_threshold and higher_better) or
                                        (score >= self.drift_threshold and not higher_better))
                                ]
            if abs_drift:
                abs_test, abs_score = abs_drift[0]
                # Only skip if we have similar existing alerts
                severity = "HIGH" if abs(abs_score) > 2 * self.drift_threshold else "MEDIUM"
                alerts.append((metric, abs_score, severity, abs_test))

        
        return alerts

    def calculate_drift(self, metrics_history: List[Dict]) -> List[DriftAlert]:
        """Calculate drift using multiple detection methods"""
        alerts = []
        
        if len(metrics_history) < 2:
            return alerts
            
        # Detect sudden drift between most recent runs
        if 'sudden' in self.drift_types:
            sudden_alerts = self.detect_sudden_drift(metrics_history[0], metrics_history[1])
            for metric, score, severity, test in sudden_alerts:
                alerts.append(DriftAlert.create(
                    metric_name=metric,
                    current_run=metrics_history[0],
                    reference_run=metrics_history[1],
                    drift_score=score,
                    drift_type='sudden',
                    severity=severity,
                    experiment_name=self.experiment_name,
                    window_size=2,
                    statistical_test=test
                ))
        
        return alerts

    def log_drift_alerts(self, alerts: List[DriftAlert]):
        """Log drift alerts to Delta table"""
        if not alerts:
            return
        
        # Store in Delta table
        spark = SparkSession.builder.getOrCreate()
        
        # Define schema for the alerts table
        schema = StructType([
            StructField("metric_name", StringType(), False),
            StructField("current_value", DoubleType(), False),
            StructField("reference_value", DoubleType(), False),
            StructField("drift_score", DoubleType(), False),
            StructField("drift_type", StringType(), False),
            StructField("severity", StringType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("run_id", StringType(), False),
            StructField("window_size", IntegerType(), False),
            StructField("statistical_test", StringType(), False),
            StructField("current_dataset_version", StringType(), False),
            StructField("current_dataset_run_id", StringType(), False),
            StructField("reference_dataset_version", StringType(), False),
            StructField("reference_dataset_run_id", StringType(), False),
            StructField("alert_json", StringType(), False)  # Store full serialized alert
        ])
        
        # Convert alerts to list of dictionaries with serialized JSON
        alert_data = []
        for alert in alerts:
            alert_dict = alert.to_dict()
            # Convert numpy float64 values to Python floats
            for key in ['current_value', 'reference_value', 'drift_score']:
                if isinstance(alert_dict[key], np.float64):
                    alert_dict[key] = float(alert_dict[key])
            alert_dict['alert_json'] = alert.to_json()
            alert_data.append(alert_dict)
        
        # Create DataFrame and write to Delta table
        df = spark.createDataFrame(alert_data, schema=schema)
        df.write.format("delta").mode("append").saveAsTable("Audit.model_drift_alerts")

    def check_drift(self, n_runs: int|None = None) -> List[DriftAlert]:
        """Main method to check for model drift"""
        print("Loading experiment runs")
        runs = self.get_recent_runs(n_runs)
        print(f"Found {len(runs)} experiment runs")
        
        print("Calculating drift")
        alerts = self.calculate_drift(runs)
        print(f"Found {len(alerts)} alerts")
        
        print("Logging drift alerts")
        self.log_drift_alerts(alerts)
        return alerts

    def get_drift_history(self, days: int = 30) -> pd.DataFrame:
        """Retrieve drift history for visualization"""
        query = f"""
        SELECT *
        FROM Audit.model_drift_alerts
        WHERE timestamp >= date_sub(current_date(), {days})
        ORDER BY timestamp DESC
        """
        return self.spark.sql(query).toPandas()

    def get_metric_trends(self, days: int = 30) -> pd.DataFrame:
        """Get metric trends over time for visualization"""
        query = f"""
        SELECT 
            metric_name,
            timestamp,
            current_value,
            reference_value,
            drift_score,
            drift_type,
            severity,
            current_dataset_version,
            reference_dataset_version
        FROM Audit.model_drift_alerts
        WHERE timestamp >= date_sub(current_date(), {days})
        ORDER BY metric_name, timestamp
        """
        return self.spark.sql(query).toPandas()

    def get_metric_history(self, days: int = 30) -> pd.DataFrame:
        """Get metric values over time from MLflow runs"""
        runs = self.get_recent_runs(n_runs=days * 2)  # Get more runs to ensure enough data
        data = []
        
        for run in runs:
            for metric in self.metrics_to_monitor:
                if metric in run['metrics']:
                    data.append({
                        'timestamp': run['timestamp'],
                        'metric_name': metric,
                        'value': run['metrics'][metric],
                        'dataset_version': run['dataset_version'],
                        'run_id': run['run_id']
                    })
        
        return pd.DataFrame(data)

    def calculate_drift_scores(self, metric_history: pd.DataFrame) -> pd.DataFrame:
        """Calculate drift scores for each metric over time"""
        drift_data = []
        
        for metric in self.metrics_to_monitor:
            metric_data = metric_history[metric_history['metric_name'] == metric].sort_values('timestamp', ascending=False)
            if len(metric_data) < 2:
                continue

            for i in range(len(metric_data)-1):
                current_value = metric_data.iloc[i]['value']
                reference_value = metric_data.iloc[i+1]['value']
                
                # Calculate drift scores using all statistical tests
                test_results = self.calculate_statistical_tests(current_value, reference_value)
                abs_score = test_results["abs_diff"]
                drift_data.append({
                        'timestamp': metric_data.iloc[i]['timestamp'],
                        'metric_name': metric,
                        'drift_score': abs_score[0],
                        'p_value': None,
                        'statistical_test': "abs_diff",
                        'current_value': current_value,
                        'reference_value': reference_value,
                        'dataset_version': metric_data.iloc[i]['dataset_version'],
                        'run_id': metric_data.iloc[i]['run_id']
                })
        
        return pd.DataFrame(drift_data)

    def plot_metric_drift(self, days: int = 30, figsize: Tuple[int, int] = (15, 10)) -> None:
        """Plot metric drift over time with thresholds and alerts"""
        
        # Get metric history and drift scores
        metric_history = self.get_metric_history()
        drift_scores = self.calculate_drift_scores(metric_history)
        
        # Get alerts for the period
        alerts = self.get_drift_history(days)
        
        # Create subplots for each metric
        n_metrics = len(self.metrics_to_monitor)
        n_cols = 2
        n_rows = (n_metrics + 1) // n_cols
        
        fig, axes = plt.subplots(n_rows, n_cols, figsize=figsize)
        if n_rows == 1:
            axes = axes.reshape(1, -1)
        
        for idx, metric in enumerate(self.metrics_to_monitor):
            row = idx // n_cols
            col = idx % n_cols
            ax = axes[row, col]
            
            # Plot metric values
            metric_data = metric_history[metric_history['metric_name'] == metric]
            ax.plot(metric_data['timestamp'], metric_data['value'], 
                   label='Metric Value', color='blue', alpha=0.7)
            

            # Plot drift scores
            drift_data = drift_scores[drift_scores['metric_name'] == metric]
            ax_twin = ax.twinx()
            ax_twin.plot(drift_data['timestamp'], drift_data['drift_score'],
                        label='Drift Score', color='red', alpha=0.7)
            
            # Plot threshold
            ax_twin.axhline(y=self.drift_threshold, color='r', linestyle='--', 
                          label='Drift Threshold', alpha=0.5)
            
            # Plot alerts
            metric_alerts = alerts[alerts['metric_name'] == metric]
            if not metric_alerts.empty:
                alert_times = pd.to_datetime(metric_alerts['timestamp'])
                alert_scores = metric_alerts['drift_score']
                ax_twin.scatter(alert_times, alert_scores, 
                              color='red', s=100, label='Alerts')
            
            # Customize plot
            ax.set_title(f'{metric} Drift Analysis')
            ax.set_xlabel('Time')
            ax.set_ylabel('Metric Value', color='blue')
            ax_twin.set_ylabel('Drift Score', color='red')
            
            # Format x-axis
            ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
            
            # Add legend
            lines1, labels1 = ax.get_legend_handles_labels()
            lines2, labels2 = ax_twin.get_legend_handles_labels()
            ax.legend(lines1 + lines2, labels1 + labels2, 
                     loc='upper right', bbox_to_anchor=(1.15, 1))
            
            # Set y-axis colors
            ax.tick_params(axis='y', labelcolor='blue')
            ax_twin.tick_params(axis='y', labelcolor='red')
        
        # Remove empty subplots if any
        for idx in range(n_metrics, n_rows * n_cols):
            row = idx // n_cols
            col = idx % n_cols
            fig.delaxes(axes[row, col])
        
        plt.tight_layout()
        return fig

    def plot_drift_summary(self, days: int = 30, figsize: Tuple[int, int] = (15, 8)) -> None:
        """Plot summary of drift scores across all metrics"""
        
        # Get drift scores
        drift_scores = self.calculate_drift_scores(self.get_metric_history(days))
        
        # Create heatmap data
        pivot_data = drift_scores.pivot(
            index='timestamp', 
            columns='metric_name', 
            values='drift_score'
        )
        
        # Create plot
        fig, ax = plt.subplots(figsize=figsize)
        
        # Plot heatmap
        im = ax.imshow(pivot_data.T, aspect='auto', cmap='RdYlGn_r')
        
        # Customize plot
        ax.set_title('Drift Score Heatmap')
        ax.set_xlabel('Time')
        ax.set_ylabel('Metrics')
        
        # Set y-axis labels
        ax.set_yticks(range(len(self.metrics_to_monitor)))
        ax.set_yticklabels(self.metrics_to_monitor)
        
        # Format x-axis
        ax.set_xticks(range(len(pivot_data.index)))
        ax.set_xticklabels(pivot_data.index.strftime('%Y-%m-%d'), rotation=45)
        
        # Add colorbar
        plt.colorbar(im, ax=ax, label='Drift Score')
        
        # Add threshold line
        ax.axhline(y=len(self.metrics_to_monitor)/2, color='white', linestyle='--', 
                  label='Threshold', alpha=0.5)
        
        plt.tight_layout()
        return fig

    def get_drift_summary(self, days: int = 30) -> pd.DataFrame:
        """Get summary of drift scores across all metrics"""
        drift_scores = self.calculate_drift_scores(self.get_metric_history(days))
        
        # Calculate summary statistics
        summary = drift_scores.groupby('metric_name').agg({
            'drift_score': ['mean', 'std', 'max', 'min'],
            'p_value': ['mean', 'min'],
            'statistical_test': lambda x: x.mode().iloc[0] if not x.empty else None
        }).round(4)
        
        # Add threshold information
        summary['threshold'] = self.drift_threshold
        summary['above_threshold'] = summary[('drift_score', 'max')] > self.drift_threshold
        
        return summary 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Monitor Datasets, re-training and inference

# CELL ********************

mlflow.autolog(disable=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dataset alerts

# CELL ********************

# Initialize the monitor
dataset_monitor = ModelDriftMonitor(
    experiment_name="Product_RecSys_Datasets",
    # run_name="als_model_evaluation",
    drift_threshold=0.00001,
    window_size=4,  # Consider 5 runs for trend analysis
    statistical_tests = ["abs_diff"],
    metrics_to_monitor=[
        'avg_interactions_per_user', 
        'avg_interactions_per_item'
    ],

)
dataset_monitor

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for drift
print("Checking for dataset drift...")
dataset_alerts = dataset_monitor.check_drift()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Training alerts

# CELL ********************

# Initialize the monitor
training_monitor = ModelDriftMonitor(
    experiment_name="Product_RecSys_Training",
    run_name="als_model_evaluation",
    drift_threshold=0.1,
    window_size=4,  # Consider 5 runs for trend analysis
    statistical_tests = ["abs_diff"],
    metrics_to_monitor=[
        'MAP_k1', 'MAP_k5', 'MAP_k10',
        'NDCG_k1', 'NDCG_k5', 'NDCG_k10',
        'mae', 'rmse'
    ],

)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for drift
print("Checking for model drift...")
training_alerts = training_monitor.check_drift()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

training_monitor.plot_metric_drift(figsize=(15,20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Inference alerts

# CELL ********************

# Initialize the monitor
inference_monitor = ModelDriftMonitor(
    experiment_name="Product_RecSys_Inference",
    drift_threshold=0.01,
    window_size=4,  # Consider 5 runs for trend analysis
    statistical_tests = ["abs_diff"],
    metrics_to_monitor=[
        'catalog_coverage_percent', 'recommendation_gini'
    ],

)
inference_monitor

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Check for drift
print("Checking for inference drift...")
inference_alerts = inference_monitor.check_drift()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

inference_monitor.plot_metric_drift(figsize=(15,5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
