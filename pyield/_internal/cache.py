"""Cache com expiração por tempo (TTL) para proteger APIs externas contra
chamadas repetidas acidentais (ex.: re-execução de célula em notebook).
"""

import time
from functools import wraps

_TTL_PADRAO = 60  # segundos
_TAMANHO_MAXIMO = 16


def ttl_cache(ttl: int = _TTL_PADRAO, maxsize: int = _TAMANHO_MAXIMO):
    """Decorador de cache com expiração por tempo.

    Args:
        ttl: Tempo de vida de cada entrada em segundos.
        maxsize: Número máximo de entradas no cache.
    """

    def decorador(func):
        _cache: dict = {}

        @wraps(func)
        def wrapper(*args):
            agora = time.monotonic()
            if args in _cache:
                resultado, expira_em = _cache[args]
                if agora < expira_em:
                    return resultado
            resultado = func(*args)
            _cache[args] = (resultado, agora + ttl)
            # Remove entrada mais antiga quando excede o tamanho máximo
            if len(_cache) > maxsize:
                _cache.pop(next(iter(_cache)))
            return resultado

        return wrapper

    return decorador
