import math, string
from random import Random

from .chrono import dateonly, datetime

_CHAR_POOL = string.printable + "\u03b1\u03b2\u03b3\u2020\u2021\u2022" #"\U00010330"
def _random_str(rng: Random, for_key=False):
    n = rng.randint(0, 20)
    if for_key and rng.randint(0, 1):
        # Generate within the
        return "".join(rng.choices("abcxyz+-._", k=n))
    return "".join(rng.choices(_CHAR_POOL, k=n))

def frid_random(rng: Random, depth: int=1, *, for_json: int=0):
    r = rng.randint(0, 32 if depth > 0 else 20)
    match r:
        case 0:
            return None
        case 1:
            return True
        case 2:
            return False
        case 3:
            if for_json:
                return None
            return datetime.now().replace(microsecond=0)
        case 4:
            if for_json:
                return True
            return dateonly.today()
        case 5:
            if for_json:
                return False
            return datetime.now().time().replace(microsecond=0)
        case 6 | 7 | 8 | 9:
            return _random_str(rng)
        case 10 | 11:
            if for_json:
                return _random_str(rng)
            return _random_str(rng).encode()
        case 12 | 13 | 14:
            return rng.randint(-10000, 10000)
        case 15:
            # Cannot use NaN as NaN != NaN
            if for_json == 1:
                return rng.choice([1.0, 0.0, -1.0, math.e, math.pi]) # no infinity
            return rng.choice([math.inf, 1.0, 0.0, -1.0, math.e, math.pi, -math.inf])
        case 16 | 17 | 18 | 19:
            return math.ldexp(rng.random() - 0.5, rng.randint(-40, 40))
        case 20 | 21 | 22 | 23 | 24 | 25:
            return [frid_random(rng, depth - 1, for_json=for_json)
                    for _ in range(rng.randint(0, 8))]
        case 26 | 27 | 28 | 29 | 30 | 31:
            return {
                _random_str(rng, True):
                    frid_random(rng, depth - 1, for_json=for_json)
                for _ in range(rng.randint(0, 8))
            }
