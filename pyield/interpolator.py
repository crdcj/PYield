import bisect


def interpolate_flat_forward(
    tx_ant: float, du_ant: int, tx_pos: float, du_pos: int, du: int
) -> float:
    """
    Realiza a interpolação da taxa de juros usando o método de interpolação flat forward
    considerando uma base de 252 dias úteis.

    A interpolação é realizada entre dois pares de vértices conhecidos (tx_ant, du_ant)
    e (tx_pos, du_pos), para um terceiro vértice definido pelo número de dias úteis 'du'
    em que 'du_ant < du < du_pos'. Esse terceiro vértice é o ponto no tempo para o qual
    a taxa de juros está sendo calculada.

    Args:
    - tx_ant (float): Taxa de juros do vértice anterior.
    - du_ant (int): Número de dias úteis do vértice anterior.
    - tx_pos (float): Taxa de juros do vértice posterior.
    - du_pos (int): Número de dias úteis do vértice posterior.
    - du (int): Dias úteis em que se deseja interpolar a taxa de juros.

    Exemplo de uso:
    taxa_interpolada = interpolate_flat_forward(0.045, 30, 0.05, 60, 45)

    Retorna:
        float: A taxa de juros interpolada no `du` fornecido.
    """
    a = (1 + tx_ant) ** (du_ant / 252)
    b = (1 + tx_pos) ** (du_pos / 252)
    c = (du - du_ant) / (du_pos - du_ant)

    return (a * (b / a) ** c) ** (252 / du) - 1


def find_and_interpolate_flat_forward(
    du: int,
    dus: list[int],
    txs: list[float],
) -> float:
    """
    Encontra o ponto de interpolação apropriado e retorna a taxa interpolada pelo método
    flat forward desse ponto. Utiliza o módulo `bisect` para busca binária em listas
    ordenadas.

    Args:
        du (int): Número de dias úteis em que se deseja calcular a taxa de juros flat
        forward. dus (List[int]): Lista com os dias úteis em que as taxas de juros são
        conhecidas. txs (List[float]): Lista com as taxas de juros conhecidas.

    Notas:
        - Presume-se que `dus` e `txs` estão ordenados e têm o mesmo tamanho.
        - O método utiliza 252 dias úteis por ano na interpolação, que é o padrão do
          mercado brasileiro.
        - Casos especiais são tratados para as situações onde `du` é menor que o
          primeiro DU na lista, maior que o último DU, ou exatamente igual a um DU
          conhecido, evitando a necessidade de interpolação.

    Exemplo de uso: taxa_interpolada = find_and_interpolate_flat_forward(45, [30, 60,
    90], [0.045, 0.05, 0.055])

    Retorna:
        float: A taxa de juros interpolada pelo método flat forward para o número de
        dias úteis fornecido.
    """
    # Special cases
    if du <= dus[0]:
        return txs[0]
    elif du >= dus[-1]:
        return txs[-1]
    # Do not interpolate vertex whose rate is known
    elif du in dus:
        return txs[dus.index(du)]

    # Find i such that du[i-1] < du < du[i]
    i = bisect.bisect_left(dus, du)

    return interpolate_flat_forward(txs[i - 1], dus[i - 1], txs[i], dus[i], du)
