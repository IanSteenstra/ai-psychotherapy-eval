"""AI Psychotherapy Evaluation: simulate multi-session therapy with AI therapists and measure quality of care and risk."""

from .config import ConfigError, load_config
from .simulation import Simulation
from .therapists import TherapistRequest

__version__ = "1.0.0"
__all__ = ["ConfigError", "Simulation", "TherapistRequest", "load_config", "__version__"]
