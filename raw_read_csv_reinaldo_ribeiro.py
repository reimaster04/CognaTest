import pandas as pd
from collections import defaultdict
import psutil
import time
#######################################
# Author: Reinaldo Ribeiro de Paula Data 15/04/2024
# Leitura do arquivo CSV vendas
# Inicializando contadores e variáveis
produto_mais_vendido = defaultdict(int)
pais_maior_volume_vendas = defaultdict(float)
media_vendas_mensais = defaultdict(list)
contador_registros = 0                    # Contador para o número de registros processados

def get_year_month(x):
    return 100*x.year + x.month


# Lendo o arquivo CSV em partes
chunksize = 10 ** 6  # Leitura de 1 milhao por vez para minimizar a utilizacao da RAM
for my_chunk in pd.read_csv('vendas.csv', chunksize=chunksize):
    
    start_time = time.time()  # Inicia o cronômetro

    # Atualizando o contador de registros
    contador_registros += len(my_chunk)

    # Monitorando o uso de CPU e memória
    cpu_percent = psutil.cpu_percent()
    memory_percent = psutil.virtual_memory().percent
    


    # Identificando o produto mais vendido
    produtos = my_chunk.groupby(['Item Type', 'Sales Channel'])['Units Sold'].sum()
    for idx, val in produtos.items():
        produto_mais_vendido[idx] += val

    # Determinando o país e a região com o maior volume de vendas
    vendas = my_chunk.groupby(['Country', 'Region'])['Total Revenue'].sum()
    for idx, val in vendas.items():
        pais_maior_volume_vendas[idx] += val

    # Calculando a média de vendas mensais por produto
    my_chunk['Order Date'] = pd.to_datetime(my_chunk['Order Date'])
    my_chunk['YearMonth'] = my_chunk['Order Date'].map(get_year_month)
    vendas_mensais = my_chunk.groupby(['YearMonth', 'Item Type'])['Units Sold'].mean()
    for idx, val in vendas_mensais.items():
        media_vendas_mensais[idx[1]].append(val)

    end_time = time.time()  # Para o cronômetro
    elapsed_time = end_time - start_time  # Calcula o tempo decorrido

    print(f'Registros processados: {contador_registros}, Uso de CPU: {cpu_percent}%, Uso de Memória: {memory_percent}%, Tempo decorrido: {elapsed_time} segundos')
    # Pausa para permitir que o monitoramento de recursos seja mais preciso
    time.sleep(1)

# Processando os resultados
produto_mais_vendido = max(produto_mais_vendido, key=produto_mais_vendido.get)
pais_maior_volume_vendas = max(pais_maior_volume_vendas, key=pais_maior_volume_vendas.get)
media_vendas_mensais = {k: sum(v)/len(v) for k, v in media_vendas_mensais.items()}

print(f'Produto mais vendido: {produto_mais_vendido}')
print(f'País e região com maior volume de vendas: {pais_maior_volume_vendas}')
print(f'Média de vendas mensais por produto: {media_vendas_mensais}')