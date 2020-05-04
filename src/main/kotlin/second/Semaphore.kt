package second
import java.math.BigInteger
import java.util.concurrent.*
import kotlin.concurrent.thread


fun main(args: Array<String>){
    semaphore()
}

/**
 * Реализация семафора
 */
fun semaphore() {

    val sem = Semaphore(1) //в один момент времени к данным имеет доступ только 1 поток
    val listOfNumbers = getData("data.txt").toMutableList() //собираем данные
    var currentNumber: BigInteger? //текущее число для разложения
    var listFrom = mutableListOf<BigInteger>() //вспомагательный список
    var stop = false

    //задание
    val r = {
        while (!stop) {
            try {
                sem.acquire() //обращаемся к данным
             //   println("Semaphore acquired " + Thread.currentThread().id)
                //проверяем, есть ли еще числа в списке
                if(listOfNumbers.isNotEmpty()) {
                    currentNumber = listOfNumbers[0]
                    listOfNumbers.removeAt(0)
                    listFrom.add(currentNumber!!)
                }
                TimeUnit.MICROSECONDS.sleep(100)

            } finally {
                if(listFrom.isNotEmpty()) {
                    //получаем текущее число
                    val el = listFrom[0]
                    listFrom = mutableListOf<BigInteger>()
                    sem.release() //освобождаем ресурс для доступа для других потоков
                    val fact = arrayListOf<Int>() //очищаем
                    factorization(el, fact) //проводим вычисления
                //    println("Semaphore released " + Thread.currentThread().id)
                }
                else {sem.release(); stop = true}
            }
        }
    }

    startAndJoin(r, r, r, r, r, r) //создаем 6 потоков
}

fun startAndJoin(vararg blocks: () -> Unit) {
    val startTime = System.currentTimeMillis()
    val threads = blocks.map { thread(block = it, isDaemon = true, start = true)}
    threads.forEach {it.join()}
    val finishTime = System.currentTimeMillis()
    println("Затрачено времени(в мс):${finishTime-startTime}")
    println("Количество чисел:${list.size}")
    println("Ответ:${getCount()}")
}