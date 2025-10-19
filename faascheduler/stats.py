from collections import defaultdict

base_stats: dict = {}


def stats_base_dict() -> dict:
    return base_stats.copy()


stats: dict = defaultdict(stats_base_dict)
