package second

import java.math.BigInteger
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

fun main(args:Array<String>){
    val startTime = System.currentTimeMillis();
    start()
    finish()
    val finishTime = System.currentTimeMillis();
    println("Затрачено ${finishTime - startTime} мс")
}

fun start(){
    val dataList = getData("data.txt").toMutableList();
    val numThreads = 10;
    val executorService = Executors.newFixedThreadPool(numThreads); //10 потоков
    val parts = dataList.chunked(dataList.size / numThreads); //делим на 10 частей
    val futures = parts.map{data ->
        executorService.submit{
            factorize(data)}} //возвращаем статусы исполнения потоков
    //пока вычисление не закончилось, ждем
    futures.map { future ->
        while (!future.isDone) {
            //println("Waiting...")
            TimeUnit.MICROSECONDS.sleep(100)
        }
    }
    executorService.shutdown() //останавливаем потоки

}

fun finish(){
    println("work finished")
    println(getCount()) //считаем суммарное количество
}

fun factorize(list:List<BigInteger>){
    list.map { number ->
        val fact = arrayListOf<Int>()
        factorization(number,fact)
    }
}