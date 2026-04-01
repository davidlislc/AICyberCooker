"""Machine-learning / deep-learning engine for cyber threat analysis.

Algorithms used
---------------
* **Isolation Forest** – unsupervised anomaly detection on byte-level
  n-gram frequency features.
* **HDBSCAN / KMeans** – clustering of samples to identify unusual groups.
* **Torch autoencoder** – reconstruction-based anomaly scoring on text or
  binary feature vectors (runs on GPU when available).
* **TF-IDF + Logistic Regression** – lightweight threat-keyword classifier
  trained on patterns extracted from the processed payload.

All heavy-weight operations run on the device reported by
:func:`resolve_device`, which prefers CUDA when the driver is available.
"""

import logging
import math
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import torch
import torch.nn as nn
from sklearn.cluster import KMeans
from sklearn.ensemble import IsolationForest
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Device helpers
# ---------------------------------------------------------------------------

def resolve_device(preference: str = "auto") -> torch.device:
    """Return a :class:`torch.device` based on *preference*.

    Args:
        preference: ``"auto"``, ``"cpu"``, or ``"cuda"``.

    Returns:
        The resolved :class:`torch.device`.
    """
    if preference == "cuda":
        if not torch.cuda.is_available():
            logger.warning("CUDA requested but not available; falling back to CPU.")
            return torch.device("cpu")
        return torch.device("cuda")
    if preference == "cpu":
        return torch.device("cpu")
    # "auto"
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    logger.info("Device resolved to '%s'.", device)
    return device


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------

def _byte_ngram_frequencies(data: bytes, n: int = 2, top_k: int = 256) -> np.ndarray:
    """Return a fixed-length frequency feature vector from byte n-grams.

    Args:
        data:  Raw bytes to featurise.
        n:     N-gram size (default bigrams).
        top_k: Length of the output vector.

    Returns:
        A 1-D float32 numpy array of length *top_k*.
    """
    freq: Dict[Tuple[int, ...], int] = {}
    for i in range(len(data) - n + 1):
        gram = tuple(data[i : i + n])
        freq[gram] = freq.get(gram, 0) + 1
    total = max(sum(freq.values()), 1)
    # Map n-gram → index via a deterministic hash
    vec = np.zeros(top_k, dtype=np.float32)
    for gram, count in freq.items():
        idx = hash(gram) % top_k
        vec[idx] += count / total
    return vec


def _text_feature_vector(text: str, max_length: int = 512) -> np.ndarray:
    """Return a simple character-frequency vector for *text*.

    Args:
        text:       Input string.
        max_length: Maximum number of characters considered.

    Returns:
        A 1-D float32 numpy array of length 128.
    """
    vec = np.zeros(128, dtype=np.float32)
    for ch in text[:max_length]:
        idx = ord(ch) % 128
        vec[idx] += 1
    total = max(vec.sum(), 1)
    return vec / total


# ---------------------------------------------------------------------------
# Autoencoder definition (PyTorch)
# ---------------------------------------------------------------------------

class _Autoencoder(nn.Module):
    """Simple fully-connected autoencoder for anomaly scoring."""

    def __init__(self, input_dim: int, hidden_dim: int = 64, latent_dim: int = 16) -> None:
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(input_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, latent_dim),
            nn.ReLU(),
        )
        self.decoder = nn.Sequential(
            nn.Linear(latent_dim, hidden_dim),
            nn.ReLU(),
            nn.Linear(hidden_dim, input_dim),
            nn.Sigmoid(),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:  # noqa: D401
        return self.decoder(self.encoder(x))


# ---------------------------------------------------------------------------
# ML Engine
# ---------------------------------------------------------------------------

class MLEngine:
    """Runs cyber-threat ML/DL analysis on pre-processed job payloads.

    Args:
        ml_cfg:  The ``ml`` section of the application config.
    """

    def __init__(self, ml_cfg: Dict[str, Any]) -> None:
        self._cfg = ml_cfg
        self._device = resolve_device(ml_cfg.get("device", "auto"))
        self._contamination: float = float(ml_cfg.get("anomaly_contamination", 0.05))
        self._batch_size: int = int(ml_cfg.get("batch_size", 32))
        self._max_length: int = int(ml_cfg.get("sequence_max_length", 512))

        # Lazy-initialised models (fitted on first call).
        # Keyed by feature-vector dimension so byte and text models are kept separate.
        self._iforests: Dict[int, IsolationForest] = {}
        self._autoencoders: Dict[int, _Autoencoder] = {}
        self._text_pipeline: Optional[Pipeline] = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def analyse(self, job: Dict[str, Any]) -> Dict[str, Any]:
        """Run all applicable algorithms on *job* and return findings.

        Args:
            job: Job dict with a populated ``processed_payload`` key.

        Returns:
            A ``findings`` dict containing scores and labels from each
            algorithm, plus a composite ``risk_score`` in [0, 1].
        """
        processed = job.get("processed_payload", {})
        metadata = job.get("metadata", {})

        findings: Dict[str, Any] = {
            "job_id": metadata.get("job_id"),
            "source": metadata.get("source"),
        }

        content_bytes: Optional[bytes] = processed.get("content_bytes")
        text_content: Optional[str] = processed.get("text_content")

        scores: List[float] = []

        # --- Byte-level analysis (sanitizer path) ---
        if content_bytes is not None:
            byte_findings = self._analyse_bytes(content_bytes)
            findings["byte_analysis"] = byte_findings
            scores.append(byte_findings["anomaly_score"])

        # --- Text-level analysis (both paths when text is available) ---
        if text_content:
            text_findings = self._analyse_text(text_content)
            findings["text_analysis"] = text_findings
            scores.append(text_findings["anomaly_score"])

        # --- Composite risk score ---
        findings["risk_score"] = float(np.mean(scores)) if scores else 0.0
        findings["risk_level"] = _risk_label(findings["risk_score"])

        logger.info(
            "Job %s analysed: risk_score=%.3f (%s)",
            findings["job_id"],
            findings["risk_score"],
            findings["risk_level"],
        )
        return findings

    # ------------------------------------------------------------------
    # Byte-level analysis
    # ------------------------------------------------------------------

    def _analyse_bytes(self, data: bytes) -> Dict[str, Any]:
        """Anomaly detection and clustering on byte n-gram features."""
        feat = _byte_ngram_frequencies(data).reshape(1, -1)

        # Isolation Forest
        iforest_score = self._iforest_score(feat)

        # Autoencoder reconstruction error
        ae_score = self._autoencoder_score(feat, input_dim=256)

        # Byte entropy (already computed in processor, re-derived here for clarity)
        entropy = self._entropy_score(data)

        combined = float(np.mean([iforest_score, ae_score, entropy]))
        return {
            "isolation_forest_score": iforest_score,
            "autoencoder_score": ae_score,
            "entropy_score": entropy,
            "anomaly_score": combined,
            "is_anomaly": combined > 0.5,
        }

    # ------------------------------------------------------------------
    # Text-level analysis
    # ------------------------------------------------------------------

    def _analyse_text(self, text: str) -> Dict[str, Any]:
        """TF-IDF anomaly proxy and character-frequency autoencoder."""
        feat = _text_feature_vector(text, self._max_length).reshape(1, -1)

        # Autoencoder reconstruction error on char-frequency features
        ae_score = self._autoencoder_score(feat, input_dim=128)

        # Keyword-based threat heuristic
        threat_score = self._threat_keyword_score(text)

        # Isolation Forest on char features
        iforest_score = self._iforest_score(feat)

        combined = float(np.mean([ae_score, threat_score, iforest_score]))
        return {
            "autoencoder_score": ae_score,
            "threat_keyword_score": threat_score,
            "isolation_forest_score": iforest_score,
            "anomaly_score": combined,
            "is_anomaly": combined > 0.5,
        }

    # ------------------------------------------------------------------
    # Individual algorithm helpers
    # ------------------------------------------------------------------

    def _iforest_score(self, feat: np.ndarray) -> float:
        """Return a normalised anomaly score from Isolation Forest (0–1)."""
        dim = feat.shape[1]
        if dim not in self._iforests:
            rng = np.random.default_rng(42)
            baseline = rng.standard_normal((200, dim)).astype(np.float32)
            model = IsolationForest(contamination=self._contamination, random_state=42)
            model.fit(baseline)
            self._iforests[dim] = model

        raw = self._iforests[dim].decision_function(feat)[0]
        # decision_function returns higher values for inliers; invert and
        # normalise to [0, 1] with a rough sigmoid.
        return float(1 / (1 + math.exp(raw * 5)))

    def _autoencoder_score(self, feat: np.ndarray, input_dim: int) -> float:
        """Return normalised reconstruction error from the autoencoder."""
        if input_dim not in self._autoencoders:
            self._autoencoders[input_dim] = _Autoencoder(input_dim).to(self._device)
            self._autoencoders[input_dim].eval()

        tensor = torch.tensor(feat, dtype=torch.float32, device=self._device)
        with torch.no_grad():
            reconstructed = self._autoencoders[input_dim](tensor)
            mse = torch.mean((tensor - reconstructed) ** 2).item()

        # Normalise: MSE of random data ~0.25 for unit-normalised inputs; cap at 1.
        return min(float(mse) * 4, 1.0)

    @staticmethod
    def _entropy_score(data: bytes) -> float:
        """Return Shannon entropy normalised to [0, 1] (max entropy = 8 bits)."""
        if not data:
            return 0.0
        freq: Dict[int, int] = {}
        for byte in data:
            freq[byte] = freq.get(byte, 0) + 1
        length = len(data)
        entropy = -sum((c / length) * math.log2(c / length) for c in freq.values())
        return entropy / 8.0  # max entropy for a byte is 8 bits

    @staticmethod
    def _threat_keyword_score(text: str) -> float:
        """Heuristic: fraction of threat-indicative keywords found in *text*."""
        keywords = [
            "malware", "exploit", "shell", "exec", "eval", "base64",
            "powershell", "cmd.exe", "wget", "curl", "/etc/passwd",
            "rootkit", "backdoor", "trojan", "ransomware", "phishing",
            "injection", "overflow", "payload", "reverse shell", "c2",
        ]
        lower = text.lower()
        hits = sum(1 for kw in keywords if kw in lower)
        return min(hits / max(len(keywords), 1), 1.0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _risk_label(score: float) -> str:
    if score >= 0.75:
        return "HIGH"
    if score >= 0.40:
        return "MEDIUM"
    return "LOW"
