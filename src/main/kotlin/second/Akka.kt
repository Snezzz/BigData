package second

import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.UntypedAbstractActor
import akka.routing.RoundRobinPool
import java.math.BigInteger

//
data class Number(val listOfNumbers: MutableList<BigInteger>)

class Start

fun main(args:Array<String>) {
    val startTime = System.currentTimeMillis()
    val system = ActorSystem.create("MySystem") //создаем систему акторов
    val masterActor = system.actorOf(Props.create(MasterActor::class.java),
            "master") //создаем главого актора
    masterActor.tell(Start(), null) //сообщаем главному актору о начале работы

    //при окончании работы всех акторов выводим результат
    system.whenTerminated.thenRun { ->
        val finishTime = System.currentTimeMillis()
        println("Затрачено времени(в мс):${finishTime-startTime}")
        println("Количество чисел:${list.size}")
        println("Ответ:${getCount()}")
    }

}

/**
 * Реализация второстепенного, который производит расчеты
 * по полученным данным
 */
class DownloaderActor: UntypedAbstractActor() {

    //получаем сообщение
    override fun onReceive(message: Any?) {
        //определяем, что необходимо сделать
        when(message) {
            is Number -> {
                //проводим вычисления
                for (el in message.listOfNumbers) {
                    val fact = arrayListOf<Int>() //очищаем
                    factorization(el, fact)
                }
                println("I finished")
                context.system.terminate() //говорим об окончании работы
            }
        }
    }
}

/**
 * Реализация главного актора
 */
class MasterActor: UntypedAbstractActor() {

    private val actorsCount = 6 //говорим, сколько потоков должны выполнять текущее задание
    private val worker = context.actorOf(RoundRobinPool(actorsCount).props(Props.create(DownloaderActor::class.java)),
                                        "downloader")

    override fun onReceive(message: Any?) {
        when(message) {
            is Start -> {
                val listOfNumbers = getData("data.txt").toMutableList() //берем данне
                val n = listOfNumbers.size
                //создаем список списков, что передаются в каждом сообщении
                val resultList = createDataForActors(listOfNumbers, actorsCount, n)

                //создаем сообщения
                for (list in resultList){
                    worker.tell(Number(list), self)
                }
            }
        }
    }
}

/**
 * Разбиение всего массива чисел на несколько частей
 *
 * @param listOfNumbers - исходным массив данных для разбиения
 * @param actorsCount - количество потоков
 * @param n - размер исходного массива
 */
fun createDataForActors(listOfNumbers: MutableList<BigInteger>,
                        actorsCount: Int, n:Int): MutableList<MutableList<BigInteger>>{

    val resultList:MutableList<MutableList<BigInteger>> = mutableListOf()
    var finish = getFinishNumber(0, n, actorsCount)
    var start = 0
    for (i in (0..(actorsCount - 1))) {
        val listForWork = mutableListOf<BigInteger>()

        if (i == actorsCount - 1 ) {
            for (el in (start..(n - 1))) {
                listForWork.add(listOfNumbers[el])
            }
        } else {
            for (el in (start..finish)) {
                listForWork.add(listOfNumbers[el])
            }
        }
        resultList.add(listForWork)
        start = finish + 1 //сдвиг
        finish = getFinishNumber(start, n, actorsCount)
    }
    return resultList
}

/**
 * Определение правой границы
 *
 * @param currentStep - левая граница
 * @param n - общее количество чисел
 * @param count - количество потоков
 */
fun getFinishNumber(currentStep:Int, n: Int, count: Int):Int{
    return currentStep+n/count
}