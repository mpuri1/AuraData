import numpy as np
import pandas as pd
import os
import json
from datetime import datetime

class PrivacyAnomalyDetector:
    """
    Staff-Level Privacy Anomaly Detection.
    Uses Z-score monitoring to detect anomalous spikes in sensitive data access.
    """
    
    def __init__(self, history_file="prn_docs/privacy_history.json"):
        self.history_file = history_file
        self._ensure_history_exists()

    def _ensure_history_exists(self):
        if not os.path.exists(os.path.dirname(self.history_file)):
            os.makedirs(os.path.dirname(self.history_file), exist_ok=True)
        if not os.path.exists(self.history_file):
            with open(self.history_file, "w") as f:
                json.dump([], f)

    def log_session_risk(self, risk_score: float):
        """Logs a new session risk score to history."""
        with open(self.history_file, "r") as f:
            history = json.load(f)
        
        history.append({
            "timestamp": datetime.now().isoformat(),
            "risk_score": risk_score
        })
        
        # Keep only last 100 sessions
        with open(self.history_file, "w") as f:
            json.dump(history[-100:], f)

    def calculate_z_score(self, current_risk: float) -> float:
        """
        Calculates the Z-score of the current risk compared to history.
        Formula: (x - mean) / std_dev
        """
        with open(self.history_file, "r") as f:
            history = json.load(f)
        
        if len(history) < 5:
            return 0.0 # Insufficient data for anomaly detection
            
        scores = [h["risk_score"] for h in history]
        mean = np.mean(scores)
        std = np.std(scores)
        
        if std == 0:
            return 0.0
            
        return (current_risk - mean) / std

    def check_for_anomaly(self, current_risk: float, threshold: float = 2.5) -> dict:
        """
        Returns a detailed anomaly report.
        """
        z_score = self.calculate_z_score(current_risk)
        is_anomaly = abs(z_score) > threshold
        
        return {
            "z_score": round(float(z_score), 2),
            "is_anomaly": is_anomaly,
            "severity": "CRITICAL" if z_score > 3.0 else "WARNING" if is_anomaly else "NORMAL",
            "timestamp": datetime.now().isoformat()
        }
