import math
from typing import Iterable, List


def cosine_similarity(vec1: Iterable[float], vec2: Iterable[float]) -> float:
    v1: List[float] = list(vec1)
    v2: List[float] = list(vec2)
    if len(v1) != len(v2) or not v1:
        return 0.0
    dot = sum(a * b for a, b in zip(v1, v2))
    norm1 = math.sqrt(sum(a * a for a in v1))
    norm2 = math.sqrt(sum(b * b for b in v2))
    if norm1 == 0.0 or norm2 == 0.0:
        return 0.0
    return dot / (norm1 * norm2)
