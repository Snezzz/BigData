import mmap
import os
import random
import numpy as np
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import concurrent.futures


def get_time(fun):
    start_time = time.clock()
    fun()
    final_time = time.clock() - start_time
    return str(final_time) + " seconds"
	
	
def thread_calculations(mm,l):   
    
    finalSum = 0
    max_el = 0
    min_el = 10**32
    
    for i in range(0, l):
        arr = mm[i*32: (i+1)*32]
        for el in arr:
            if el > max_el:
                max_el = el
            if el < min_el:
                min_el = el
            finalSum += el
    return max_el,min_el,finalSum
	
def make_threads(mm, el_count):
    num_processor = 7
    l = el_count//num_processor
    indexA = 0 
    indexB = l
    i = 1
    futures = []
    with ProcessPoolExecutor(num_processor) as ex:
        for future_num in range(num_processor):
            if future_num == num_processor-1:
                futures.append(ex.submit(thread_calculations, mm[indexA:], l))
            else:
                futures.append(ex.submit(thread_calculations, mm[indexA:indexB], l))
            indexA = i * l
            i+=1
            indexB = i * l
    return futures

def calc():
	f_name = 'data.txt'
	length = 90000000
	with open(f_name, 'r+b') as f:
                buff = mmap.mmap(f.fileno(), length=length, offset=0, access=mmap.ACCESS_READ)
                array = np.frombuffer(buff, dtype=np.dtype('uint32').newbyteorder('B'))
                futures = make_threads(array, length)
	final_sum = 0
	final_max = 0
	final_min = 10**32
	for future in concurrent.futures.as_completed(futures):
		current_max, current_min, current_sum = future.result()
		final_max = max(current_max,final_max)
		final_min = min(final_min,current_min)
		final_sum += current_sum
	print('Минимальный элемент:', final_min)
	print('Максимальный элемент:', final_max)
	print('Сумма элементов:', final_sum)


if __name__ == '__main__':
    print(get_time(calc))









